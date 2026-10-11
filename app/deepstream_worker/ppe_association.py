import math


# Your model's class mapping.
CLASS_MAP = {
    0: ("helmet", "PRESENT"),
    1: ("mask", "PRESENT"),
    2: ("helmet", "MISSING"),
    3: ("mask", "MISSING"),
    4: ("vest", "MISSING"),
    7: ("vest", "PRESENT"),
}

PERSON_CLASS_ID = 5
UNTRACKED_ID = (1 << 64) - 1

# Illustrative starting settings; tune against your footage.
# Values: minimum relative Y, maximum relative Y, expected Y.
REGIONS = {
    "helmet": (0.00, 0.30, 0.10),
    "mask": (0.05, 0.35, 0.20),
    "vest": (0.20, 0.70, 0.45),
}


class PPEAssociation:
    def __init__(
        self,
        min_confidence=0.40,
        min_ioa=0.50,
        min_match_score=0.65,
        ambiguity_margin=0.08,
        conflict_margin=0.15,
        allow_tracker_only_persons=True,
    ):
        self.min_confidence = min_confidence
        self.min_ioa = min_ioa
        self.min_match_score = min_match_score
        self.ambiguity_margin = ambiguity_margin
        self.conflict_margin = conflict_margin
        self.allow_tracker_only_persons = allow_tracker_only_persons

    @staticmethod
    def valid_box(box):
        """Boxes use (left, top, width, height)."""
        return (
            len(box) == 4
            and all(math.isfinite(value) for value in box)
            and box[2] > 0
            and box[3] > 0
        )

    @staticmethod
    def ioa(ppe_box, person_box):
        ex, ey, ew, eh = ppe_box
        px, py, pw, ph = person_box

        intersection_width = max(
            0.0,
            min(ex + ew, px + pw) - max(ex, px),
        )
        intersection_height = max(
            0.0,
            min(ey + eh, py + ph) - max(ey, py),
        )

        return (
            intersection_width * intersection_height
        ) / (ew * eh)

    def match_score(self, ppe_box, person_box, equipment):
        containment = self.ioa(ppe_box, person_box)

        if containment < self.min_ioa:
            return None

        ex, ey, ew, eh = ppe_box
        px, py, pw, ph = person_box

        center_x = ex + ew / 2
        center_y = ey + eh / 2

        relative_x = (center_x - px) / pw
        relative_y = (center_y - py) / ph

        y_min, y_max, expected_y = REGIONS[equipment]

        # Body-region gate.
        if not (
            0.0 <= relative_x <= 1.0
            and y_min <= relative_y <= y_max
        ):
            return None

        # Spatial relationship: distance to expected body position.
        distance = math.hypot(
            relative_x - 0.50,
            relative_y - expected_y,
        )
        proximity = 1.0 / (1.0 + distance)

        # Region score is 1 because this candidate passed the gate.
        return (    
            0.50 * containment
            + 0.30
            + 0.20 * proximity
        )

    def resolve(self, candidates):
        """Resolve all evidence for one person's equipment."""
        unknown = {"state": "UNKNOWN", "confidence": None}

        if not candidates:
            return unknown

        # Consolidate agreeing detections using strongest evidence.
        best_by_state = {}

        for candidate in candidates:
            state = candidate["state"]
            previous = best_by_state.get(state)

            if (
                previous is None
                or candidate["quality"] > previous["quality"]
            ):
                best_by_state[state] = candidate

        ranked = sorted(
            best_by_state.values(),
            key=lambda item: item["quality"],
            reverse=True,
        )

        # Contradictory evidence with no clear winner.
        if (
            len(ranked) > 1
            and ranked[0]["quality"] - ranked[1]["quality"]
            < self.conflict_margin
        ):
            return unknown

        winner = ranked[0]

        return {
            "state": winner["state"],
            # Keep original detector confidence in the output.
            "confidence": round(winner["confidence"], 4),
        }

    def associate(self, camera_id, timestamp, objects):
        persons = []

        for obj in objects:
            confidence = float(obj["confidence"])

            if (
                obj["class_id"] == PERSON_CLASS_ID
                and obj.get("track_id") is not None
                and 0 <= int(obj["track_id"]) < UNTRACKED_ID
                and self.valid_box(obj["bbox"])
                and math.isfinite(confidence)
                and (
                    self.min_confidence <= confidence <= 1.0
                    or (
                        self.allow_tracker_only_persons
                        # DeepStream uses -0.1 for tracker-only metadata.
                        # A valid track can anchor fresh PPE detections; this
                        # sentinel is never used as PPE confidence below.
                        and math.isclose(confidence, -0.1, abs_tol=1e-6)
                    )
                )
            ):
                persons.append(obj)

        # Separate candidate storage for each person.
        evidence = [
            {"helmet": [], "mask": [], "vest": []}
            for _ in persons
        ]

        for obj in objects:
            mapping = CLASS_MAP.get(obj["class_id"])

            if mapping is None:
                continue

            confidence = float(obj["confidence"])

            if (
                not math.isfinite(confidence)
                or not self.min_confidence <= confidence <= 1.0
                or not self.valid_box(obj["bbox"])
            ):
                continue

            equipment, state = mapping
            matches = []

            for person_index, person in enumerate(persons):
                score = self.match_score(
                    obj["bbox"],
                    person["bbox"],
                    equipment,
                )

                if score is not None:
                    matches.append((score, person_index))

            matches.sort(reverse=True)

            if not matches:
                continue

            best_score, owner_index = matches[0]

            if best_score < self.min_match_score:
                continue

            # Do not force ownership between overlapping people.
            if (
                len(matches) > 1
                and best_score - matches[1][0]
                < self.ambiguity_margin
            ):
                continue

            evidence[owner_index][equipment].append({
                "state": state,
                "confidence": confidence,
                # Heuristic evidence strength, not a probability.
                "quality": confidence * best_score,
            })

        observations = []

        for person, person_evidence in zip(persons, evidence):
            observations.append({
                "camera_id": str(camera_id),
                "track_id": int(person["track_id"]),
                "timestamp": float(timestamp),
                "ppe": {
                    equipment: self.resolve(candidates)
                    for equipment, candidates
                    in person_evidence.items()
                },
            })

        return observations
