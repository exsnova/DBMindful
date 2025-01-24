# core/query_analyzer.py
import sqlparse
from typing import Dict, List

class QueryAnalyzer:
    def __init__(self):
        pass

    def analyze_query(self, query: str) -> Dict:
        """Analyze a single query for potential issues"""
        try:
            # Parse the query
            parsed = sqlparse.parse(query)[0]
            
            # Extract basic information
            tables = self._extract_tables(parsed)
            joins = self._analyze_joins(parsed)
            where_clause = self._has_where_clause(parsed)
            indexes = self._suggest_indexes(parsed)
            
            # Assess complexity
            complexity = self._assess_complexity(parsed, joins)
            
            # Generate improvements
            improvements = self._generate_improvements(
                parsed, tables, joins, where_clause, indexes
            )
            
            return {
                'complexity': complexity,
                'tables_involved': tables,
                'suggested_improvements': improvements,
                'type': parsed.get_type()
            }
        except Exception as e:
            return {
                'complexity': 'Unknown',
                'tables_involved': [],
                'suggested_improvements': [f'Error analyzing query: {str(e)}'],
                'type': 'Unknown'
            }

    def _extract_tables(self, parsed_query) -> List[str]:
        """Extract table names from query"""
        tables = []
        for token in parsed_query.tokens:
            if token.ttype is None:
                if hasattr(token, 'get_real_name'):
                    name = token.get_real_name()
                    if name:
                        tables.append(name)
        return list(set(tables))

    def _analyze_joins(self, parsed_query) -> List[str]:
        """Analyze JOIN clauses"""
        joins = []
        for token in parsed_query.tokens:
            if token.is_group:
                for subtoken in token.tokens:
                    if str(subtoken).upper().strip().startswith('JOIN'):
                        joins.append(str(subtoken))
        return joins

    def _has_where_clause(self, parsed_query) -> bool:
        """Check if query has WHERE clause"""
        for token in parsed_query.tokens:
            if token.is_group:
                for subtoken in token.tokens:
                    if str(subtoken).upper().strip().startswith('WHERE'):
                        return True
        return False

    def _suggest_indexes(self, parsed_query) -> List[str]:
        """Suggest potential indexes"""
        suggestions = []
        where_columns = self._extract_where_columns(parsed_query)
        join_columns = self._extract_join_columns(parsed_query)
        
        for col in where_columns:
            suggestions.append(f"Consider index on {col}")
        for cols in join_columns:
            suggestions.append(f"Consider composite index on {', '.join(cols)}")
            
        return suggestions

    def _extract_where_columns(self, parsed_query) -> List[str]:
        """Extract columns used in WHERE clause"""
        columns = []
        for token in parsed_query.tokens:
            if token.is_group:
                for subtoken in token.tokens:
                    if str(subtoken).upper().strip().startswith('WHERE'):
                        # Extract column names from WHERE clause
                        where_tokens = str(subtoken).split()
                        for i, token in enumerate(where_tokens):
                            if '.' in token and '(' not in token:
                                columns.append(token.split('.')[-1])
        return list(set(columns))

    def _extract_join_columns(self, parsed_query) -> List[List[str]]:
        """Extract columns used in JOIN conditions"""
        join_conditions = []
        for token in parsed_query.tokens:
            if token.is_group:
                for subtoken in token.tokens:
                    if 'JOIN' in str(subtoken).upper():
                        on_clause = str(subtoken).upper().split('ON')
                        if len(on_clause) > 1:
                            columns = []
                            for col in on_clause[1].split('='):
                                if '.' in col:
                                    columns.append(col.split('.')[-1].strip())
                            if columns:
                                join_conditions.append(columns)
        return join_conditions

    def _assess_complexity(self, parsed_query, joins) -> str:
        """Assess query complexity"""
        score = 0
        
        # Base on number of tables/joins
        score += len(joins)
        
        # Check for subqueries
        if 'SELECT' in str(parsed_query).upper().split('FROM')[0]:
            score += 2
            
        # Check for aggregations
        if any(agg in str(parsed_query).upper() for agg in ['GROUP BY', 'HAVING']):
            score += 1
            
        # Check for window functions
        if 'OVER' in str(parsed_query).upper():
            score += 2
            
        if score <= 1:
            return 'Simple'
        elif score <= 3:
            return 'Moderate'
        return 'Complex'

    def _generate_improvements(self, parsed_query, tables, joins, has_where, indexes) -> List[str]:
        """Generate improvement suggestions"""
        improvements = []
        
        # Analyze joins
        if len(joins) > 2:
            improvements.append("Consider breaking down complex joins into smaller queries or views")
            
        # Check indexes
        if indexes:
            improvements.extend(indexes)
            
        # Analyze WHERE clause
        if not has_where and len(tables) > 0:
            # Check if it's really a full table scan query
            query_str = str(parsed_query).upper()
            if 'COUNT(*)' in query_str or 'SELECT *' in query_str:
                improvements.append("Full table scan detected - this might be intentional")
            else:
                improvements.append("Consider adding appropriate filters to limit result set")
                
        # Check for SELECT *
        if 'SELECT *' in str(parsed_query).upper():
            improvements.append("Specify needed columns instead of SELECT *")
            
        return improvements