import json
from typing import Any, List, Optional

from engine.base_client import IncompatibilityError
from engine.base_client.parser import BaseConditionParser, FieldValue


class PgvectorNoHnswConditionParser(BaseConditionParser):
    def build_condition(
        self, and_subfilters: Optional[List[Any]], or_subfilters: Optional[List[Any]]
    ) -> Optional[Any]:
        clauses = []
        if or_subfilters is not None and len(or_subfilters) > 0:
            clauses.append("(" + " OR ".join(or_subfilters) + ")")
        if and_subfilters is not None and len(and_subfilters) > 0:
            clauses.append("(" + " AND ".join(and_subfilters) + ")")
        return " AND ".join(clauses)

    def build_exact_match_filter(self, field_name: str, value: FieldValue) -> Any:
        if field_name == 'labels':
            # Special case: labels is a jsonb array
            val = json.dumps(value)
            return f"{field_name} @> '{val}'::jsonb"
        else:
            val = str(value).replace("'", "''")
            return f"{field_name} = '{val}'"

    def build_range_filter(
        self,
        field_name: str,
        lt: Optional[FieldValue],
        gt: Optional[FieldValue],
        lte: Optional[FieldValue],
        gte: Optional[FieldValue],
    ) -> Any:
        clauses = []
        if lt is not None:
            clauses.append(f"{field_name} < {lt}")
        if gt is not None:
            clauses.append(f"{field_name} > {gt}")
        if lte is not None:
            clauses.append(f"{field_name} <= {lte}")
        if gte is not None:
            clauses.append(f"{field_name} >= {gte}")
        return "(" + " AND ".join(clauses) + ")"

    def build_geo_filter(
        self, field_name: str, lat: float, lon: float, radius: float
    ) -> Any:
        raise IncompatibilityError
