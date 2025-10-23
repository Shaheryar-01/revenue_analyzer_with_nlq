import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class EntityResolver:
    """Central source of truth for entity classification"""

    def __init__(self, entity_metadata: Dict[str, List[str]]):
        self.entity_metadata = {
            k: [v.lower().strip() for v in vals] for k, vals in entity_metadata.items()
        }

    def resolve_entity(self, value: str) -> Optional[str]:
        """Return which entity type (customer, product, region, unit, category) a value belongs to"""
        val = value.lower().strip()
        for entity_type, values in self.entity_metadata.items():
            if val in values:
                return entity_type
        # fallback: fuzzy partial match
        for entity_type, values in self.entity_metadata.items():
            if any(val in v for v in values):
                return entity_type
        return None

    def suggest_correction(self, value: str) -> Optional[str]:
        """Suggest similar known value if entity not found"""
        val = value.lower()
        for entity_type, values in self.entity_metadata.items():
            for v in values:
                if val[:3] in v:  # small similarity heuristic
                    return f"Did you mean '{v}' in {entity_type}?"
        return None
