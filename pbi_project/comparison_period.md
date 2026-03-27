# PBI: Comparison Period (period-over-period)

Status: PAUSED — not to break the working report.

## What was done

- Analyzed colleague's pattern in `project_ex_NF/` — two calendars, inactive relationship, USERELATIONSHIP in _prev measures
- Created `M_prev.tmdl` with _prev and _diff measures (users, sessions, orders, revenue, AOV, buyers)
- Added inactive relationship in `relationships.tmdl` (fct_events_real_sm[event_date] → HB_Date_Comp[DimDate])
- Added M_prev ref to `model.tmdl`

## What went wrong

- Existing visuals are bound to `fct_events_real_sm[event_date]` directly, not through HB_Date_Basic calendar table
- Switching slicer to calendar table broke all visuals with QuerySystemError
- The _prev measures were updated to use `ALL(fct_events_real_sm[event_date])` instead of `ALL(HB_Date_Basic[DimDate])`

## Steps to complete

- [ ] Migrate all visuals to use HB_Date_Basic[DimDate] as the date axis (instead of event_date directly)
- [ ] Verify report works with calendar-based slicer
- [ ] Add inactive relationship: fct_events_real_sm[event_date] → HB_Date_Comp[DimDate]
- [ ] Add second date slicer on HB_Date_Comp[DimDate]
- [ ] Verify M_prev measures work (users_prev, orders_prev, etc.)
- [ ] Add _diff measures to KPI cards

## How comparison works (reference)

```dax
orders_prev = CALCULATE(
    [orders],
    ALL(fct_events_real_sm[event_date]),   -- removes main period filter
    USERELATIONSHIP(                       -- activates inactive relationship
        'HB_Date_Comp'[DimDate],
        fct_events_real_sm[event_date]
    )
)

orders_diff = DIVIDE([orders] - [orders_prev], [orders_prev], 0)
```

Two independent calendars = two independent date filters. `USERELATIONSHIP` switches which one controls the fact table.

## Files

- `posylka_bq/posylka_bq.SemanticModel/definition/tables/M_prev.tmdl` — measures table (exists, needs model ref)
- Colleague's reference: `project_ex_NF/exam_NF.SemanticModel/definition/tables/M_prev.tmdl`
