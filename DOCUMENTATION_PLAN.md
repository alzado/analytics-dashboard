# Comprehensive Documentation Plan

## Overview

This document outlines the complete documentation strategy for adding in-code documentation
to the entire codebase (~40 files, ~13,000 lines).

## Scope

### Backend Python Files (10 files, ~5,000 lines)
1. **main.py** (1,522 lines, 80+ endpoints) - PRIORITY 1
2. **services/bigquery_service.py** (1,322 lines) - PRIORITY 1
3. **services/data_service.py** (1,500 lines) - PRIORITY 1
4. **services/schema_service.py** (854 lines) - PRIORITY 2
5. **services/metric_service.py** (796 lines) - PRIORITY 2
6. **services/dimension_service.py** (99 lines) - PRIORITY 3
7. **services/custom_dimension_service.py** (150 lines) - PRIORITY 3
8. **services/query_logger.py** (390 lines) - PRIORITY 3
9. **services/date_resolver.py** (220 lines) - PRIORITY 3
10. **config.py** (441 lines) - PRIORITY 3

### Frontend TypeScript Files (30+ files, ~8,000 lines)
11. **lib/api.ts** (1,058 lines, ~50 functions) - PRIORITY 1
12. **lib/types.ts** (~300 lines) - PRIORITY 2
13. **Components** (23 files, ~4,000 lines) - PRIORITY 2-3
14. **Hooks** (4 files, ~400 lines) - PRIORITY 2
15. **Contexts** (2 files, ~200 lines) - PRIORITY 2

## Documentation Standards

### Python (Google-style docstrings)

#### Module Level
```python
"""
One-line module summary.

Detailed description explaining the module's purpose, key features,
and architectural patterns.

Key Features:
    - Feature 1
    - Feature 2

Architecture:
    Description of how this module fits into the system.

Example:
    >>> from module import Class
    >>> instance = Class()
"""
```

#### Class Level
```python
class ServiceName:
    """One-line class summary.

    Detailed description of the class's responsibility and usage.

    Attributes:
        attr1: Description of attribute 1
        attr2: Description of attribute 2

    Note:
        Important notes about usage or limitations.
    """
```

#### Method Level
```python
def method_name(self, param1: str, param2: int) -> dict:
    """One-line method summary.

    Detailed description of what the method does, including
    important behavior details and edge cases.

    Args:
        param1: Description of param1 with type and constraints
        param2: Description of param2 with type and constraints

    Returns:
        Dictionary containing:
            - key1: Description of key1
            - key2: Description of key2

    Raises:
        ValueError: When param1 is invalid
        HTTPException: When operation fails with status code 400

    Example:
        >>> result = instance.method_name("test", 42)
        >>> print(result["key1"])
        "value"
    """
```

### TypeScript (JSDoc)

#### File Level
```typescript
/**
 * One-line file summary.
 *
 * Detailed description of the file's purpose and exports.
 *
 * @module path/to/file
 */
```

#### Function Level
```typescript
/**
 * One-line function summary.
 *
 * Detailed description with usage notes.
 *
 * @param param1 - Description with type info
 * @param param2 - Description with type info
 * @returns Description of return value
 * @throws {Error} When something fails
 *
 * @example
 * ```tsx
 * const result = functionName("test", 42);
 * ```
 */
```

#### Component Level
```typescript
/**
 * Component description.
 *
 * @component
 * @param props - Component props
 * @returns Rendered component
 *
 * @example
 * ```tsx
 * <ComponentName prop1="value" />
 * ```
 */
```

## Implementation Strategy

### Phase 1: Critical Backend (Week 1)
Focus on the three most important backend files that handle core functionality:

1. **main.py** - Document all 80+ API endpoints
   - Each endpoint needs: description, args, returns, raises, example
   - Focus on complex endpoints with multiple parameters
   - Document filter parameter parsing

2. **bigquery_service.py** - Document BigQuery client
   - Document dynamic SQL generation
   - Explain caching mechanisms
   - Document date clamping feature

3. **data_service.py** - Document business logic
   - Document metric calculation
   - Explain pivot table logic
   - Document custom dimension handling

### Phase 2: Schema System (Week 1-2)
Document the dynamic schema system:

4. **schema_service.py** - Schema auto-detection
5. **metric_service.py** - Formula parsing
6. **dimension_service.py** - Dimension CRUD

### Phase 3: Supporting Services (Week 2)
7. **custom_dimension_service.py**
8. **query_logger.py**
9. **date_resolver.py**
10. **config.py**

### Phase 4: Frontend API (Week 2-3)
11. **lib/api.ts** - Document all ~50 API client functions
    - Each function needs full JSDoc with examples
    - Document error handling
    - Explain complex parameters

### Phase 5: Frontend Core (Week 3)
12. **lib/types.ts** - Document all type definitions
13. **Hooks** - Document all custom hooks
14. **Contexts** - Document React contexts

### Phase 6: Components (Week 3-4)
15. **Components** - Document all 23 components
    - Complex components need detailed examples
    - Document props and state
    - Explain component interactions

## Priority Files for Immediate Documentation

### Tier 1 (MUST HAVE - Do First)
These files are the foundation of the application:

1. **backend/main.py** - API gateway
2. **backend/services/bigquery_service.py** - Data layer
3. **backend/services/data_service.py** - Business logic
4. **frontend/lib/api.ts** - API client

### Tier 2 (SHOULD HAVE - Do Second)
These files enable core features:

5. **backend/services/schema_service.py** - Dynamic schema
6. **backend/services/metric_service.py** - Formula parsing
7. **frontend/components/sections/pivot-table-section.tsx** - Main UI
8. **frontend/lib/types.ts** - Type definitions
9. **frontend/hooks/** - All custom hooks

### Tier 3 (NICE TO HAVE - Do Third)
These files are important but less critical:

10. **All remaining backend services**
11. **All remaining frontend components**
12. **Context providers**

## Estimated Effort

### Time Estimates per File
- Large files (>1000 lines): 3-4 hours
- Medium files (500-1000 lines): 1-2 hours
- Small files (<500 lines): 30-60 minutes

### Total Effort
- **Backend**: ~25-30 hours
- **Frontend**: ~30-40 hours
- **Total**: ~55-70 hours (7-9 working days)

## Deliverables

### For Each File
- [ ] Module/file-level docstring
- [ ] Class docstrings for all classes
- [ ] Method/function docstrings for all public methods
- [ ] Example usage in docstrings for complex functions
- [ ] Parameter descriptions with types
- [ ] Return value descriptions
- [ ] Exception documentation
- [ ] Type definitions documented (TypeScript)

### Final Report
- [ ] Total files documented
- [ ] Total functions/methods documented
- [ ] Coverage percentage
- [ ] List of files with incomplete documentation
- [ ] Suggestions for code improvements discovered

## Quality Checklist

For each documented function/method:
- [ ] One-line summary is clear and concise
- [ ] All parameters are documented with types
- [ ] Return value is documented
- [ ] Common exceptions are listed
- [ ] At least one usage example for complex functions
- [ ] Explanation of WHY, not just WHAT
- [ ] Edge cases and special behaviors noted

## Tools & Resources

### For Backend (Python)
- Google Python Style Guide: https://google.github.io/styleguide/pyguide.html
- PEP 257 (Docstring Conventions): https://www.python.org/dev/peps/pep-0257/

### For Frontend (TypeScript)
- TSDoc Standard: https://tsdoc.org/
- JSDoc Reference: https://jsdoc.app/

## Next Steps

1. Review DOCUMENTATION_EXAMPLES.md for complete examples
2. Start with main.py (highest priority)
3. Use templates from this document
4. Document in order of priority
5. Test documentation with IDE tooltips
6. Review and refine as you go

