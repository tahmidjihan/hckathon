# Unified Wildfire Dataset & Explorer — UI/UX and Feature Roadmap

## 1. Product direction

Turn the current map-search page into a **global wildfire intelligence dashboard**: a polished workspace for exploring fire activity, comparing periods and regions, drilling from persistent fire incidents into daily observation events, and exporting the results.

The interface should feel like a serious geospatial/scientific product—not a generic admin template. Keep the brutal-minimalist identity, but introduce a clear information hierarchy, restrained color, compact data visualizations, and responsive dashboard layouts.

### Core user questions
1. Where are fires occurring?
2. How many incidents and detections occurred in a chosen area and period?
3. How intense were they (FRP)?
4. How did activity change over time?
5. Which incidents persisted, expanded, or produced the most observations?
6. Which satellite systems contributed observations?
7. How reliable and complete is the dataset for the selected query?

### Product principles
- **Map-first, not map-only**: the map is the geographic anchor; charts, grids, filters, and detail panels explain what is happening.
- **Progressive disclosure**: show a useful overview first; reveal advanced controls and event-level detail when requested.
- **Fast by design**: aggregate and filter on the server. Never download the full Parquet dataset into the browser.
- **Every visual answers a question**: avoid decorative charts and meaningless metrics.
- **Consistent definitions**: distinguish persistent fires, daily events, and raw satellite detections in labels and tooltips.
- **Accessible contrast**: preserve the dark aesthetic while ensuring readable labels, keyboard navigation, and non-color-only status cues.

## 2. Recommended application layout

### Desktop layout
Use a dashboard shell with:
- **Top bar**: product name, global search, active date range, data freshness/version, help, and settings.
- **Left sidebar**: Overview, Explore, Compare, Fire records, Saved queries, and About/Data.
- **Main workspace**: one map instance, with a flexible panel layout around it.
- **Right-side inspector**: selected fire/event details, timeline, and actions.
- **Bottom or collapsible analytics area**: charts and result grid.

Suggested overview composition:
1. **KPI strip**: fire incidents, events, total/max FRP, and active years/coverage.
2. **Main exploration area**: map beside a compact time-series and filter panel.
3. **Analytics row**: activity by month, source contribution, and top regions.
4. **Results grid**: sortable fire records for the current query.

Do not cram every chart into the first screen. Let users collapse or resize panels.

### Mobile layout
- Keep the map full-width and give it a clear expand/collapse control.
- Put filters in a bottom sheet or slide-over drawer.
- Show KPIs as horizontally scrollable compact cards.
- Render charts one per row.
- Convert the result grid into stacked record cards or a horizontally scrollable table.
- Keep the selected-fire inspector as a full-screen sheet.

## 3. Feature catalogue

### A. Global overview dashboard
**Priority: P0 — build first**
- [ ] KPI cards for total fire incidents, total events, peak FRP, and selected-period change.
- [ ] Date-range selector with presets: 24 hours, 7 days, 30 days, year-to-date, and custom.
- [ ] Year selector that uses the existing `/years` endpoint.
- [ ] Global activity time-series chart.
- [ ] Monthly/seasonal activity chart.
- [ ] Satellite-source contribution chart (MODIS Terra/Aqua, VIIRS Suomi-NPP/NOAA-20/NOAA-21).
- [ ] A clear "current query" summary showing all active filters.
- [ ] Loading, empty, partial-data, and API-error states.

**Metric definitions**
- `fires`: persistent incident records. Use this for the primary incident count.
- `events`: near-simultaneous observation clusters. Use this for daily activity and drill-down.
- `max_frp`: maximum observed fire radiative power within a fire record.
- `sum_frp`: sum of stored FRP values. Label it carefully; do not imply it is a physically integrated energy measurement unless the data model supports that interpretation.
- `detection_count` / `event_count`: observation volume, not necessarily the number of unique physical fires.

### B. Map exploration
**Priority: P0 — improve the existing map**
- [ ] Keep one persistent Leaflet map instance in the application. Update its layers and viewport when filters change; do not recreate or duplicate the map on every search or tab render.
- [ ] Add marker clustering or server-side spatial aggregation at low zoom levels.
- [ ] Use marker size or intensity to communicate a selected metric (e.g. event count or max FRP).
- [ ] Add a legend that updates with the selected map metric.
- [ ] Add map metric modes: incident count, event count, max FRP, and source.
- [ ] Add optional heatmap mode for broad activity patterns.
- [ ] Add a rectangle/polygon selection tool if the API can support polygon/bbox queries.
- [ ] Add "fit results" and "reset view" controls.
- [ ] Add a scale bar, coordinates on pointer/move, and clear zoom controls.
- [ ] Make selected markers visually distinct and keep selection synchronized with the result grid.
- [ ] Provide a compact map layer/source control.
- [ ] Debounce map movement queries and only query after the user stops moving the map.
- [ ] Show the number of results currently represented on the map.

**Stop the world map from repeating**

Leaflet's default world behavior can make the map appear to repeat horizontally when panning beyond the antimeridian. Configure the tile layer and map bounds so the world is shown once:

```js
const map = L.map("map", {
  worldCopyJump: false,
  maxBounds: [[-85, -180], [85, 180]],
  maxBoundsViscosity: 1.0,
  minZoom: 2
});

L.tileLayer(tileUrl, {
  noWrap: true,
  bounds: [[-85, -180], [85, 180]],
  attribution: "..."
}).addTo(map);
```

Implementation notes:
- Create the map once, outside frequently re-rendered UI code.
- Keep references to the map, tile layer, marker layer, and clusters.
- On a new search, update marker data with `clearLayers()` / `addLayers()` rather than creating another map.
- If the frontend is later moved to React, initialize Leaflet in a one-time lifecycle effect and clean it up only when that component truly unmounts.
- The longitude boundary is inherently awkward at the antimeridian. Test searches around ±180° and do not silently discard valid cross-dateline results.

### C. Time-series and temporal analysis
**Priority: P0/P1**
- [ ] Fire incidents over time (daily/weekly/monthly aggregation).
- [ ] Events over time, shown separately from fire incidents.
- [ ] FRP over time (max, mean, or distribution—label the aggregation).
- [ ] Compare current period against previous period.
- [ ] Brush/zoom selection on the time-series to narrow the date range.
- [ ] Day/night or hour-of-day distribution, if event timestamps preserve sufficient time precision.
- [ ] Seasonal profile by month.
- [ ] Year-over-year comparison.
- [ ] Annotate major spikes and allow users to click a spike to filter the map and grid.

### D. Geographic analytics
**Priority: P1**
- [ ] Top countries/regions by fire incidents and events, if reliable geographic attribution is available.
- [ ] Activity by latitude band.
- [ ] Regional comparison: select two or more bounding boxes/regions and compare them.
- [ ] Density grid/hexbin layer at multiple zoom levels.
- [ ] Fire activity by administrative area, only after adding a dependable boundary dataset and spatial join.
- [ ] "Nearby activity" around a selected fire.
- [ ] Save a geographic area as a reusable query.

Do not infer country names from rough coordinates in the browser. Add a documented geographic attribution process or use a vetted boundary dataset.

### E. Fire records data grid
**Priority: P0**

Build a serious, fast data table for the `/fires` result set.
- [ ] Columns: fire ID, start date, end date, duration, centroid, event count, detection count, max FRP, sum FRP, and source systems.
- [ ] Server-side pagination, sorting, and filtering.
- [ ] Column visibility chooser and sensible default column order.
- [ ] Sticky header and compact density mode.
- [ ] Sort by max FRP, event count, duration, start date, and detection count.
- [ ] Click a row to select the same fire on the map and open the inspector.
- [ ] Copy fire ID and coordinates.
- [ ] Export the filtered result page/query as CSV or GeoJSON (server-side, with limits).
- [ ] Clear indication of current page, total matches, and active filters.
- [ ] Preserve query state when moving between map, grid, and detail views.

For 60M+ fire rows, never attempt to load all rows into a client-side grid. Use the existing `limit`/`offset` initially; consider cursor pagination for deep paging.

### F. Fire detail inspector
**Priority: P0**

When a fire is selected, open a side panel or detail route containing:
- [ ] Fire ID and key dates.
- [ ] Centroid and bounding box.
- [ ] Duration and event/detection counts.
- [ ] Max and sum FRP with clear definitions.
- [ ] Source-system badges.
- [ ] Event timeline, ordered by timestamp.
- [ ] Event list with date/time, location, FRP, and source.
- [ ] "Zoom to fire" and "copy ID" actions.
- [ ] Navigation to previous/next result.
- [ ] Deep link using the stable `FIRE-...` ID.

The event timeline must use `/fires/{fire_id}/events`; do not fetch every event in advance.

### G. Comparison workspace
**Priority: P1 — high-value differentiator**
- [ ] Compare two date ranges side by side.
- [ ] Compare two geographic areas.
- [ ] Compare years or seasons.
- [ ] Show absolute and percentage differences for incidents, events, and FRP.
- [ ] Overlay both time-series with a clear legend.
- [ ] Show a map difference mode (where activity increased/decreased).
- [ ] Allow comparison links to be shared.

This is one of the most compelling features because it turns the dataset from a lookup tool into an analysis tool.

### H. Satellite/source analysis
**Priority: P1**
- [ ] Breakdown by satellite/instrument.
- [ ] Compare MODIS and VIIRS activity for the same area and period.
- [ ] Source filter in map, grid, and charts.
- [ ] Explain sensor resolution differences (MODIS ~1 km; VIIRS ~375 m).
- [ ] Clearly disclose that cross-sensor observations have been harmonized and de-duplicated, and that source counts are observations—not independent fire incidents.

### I. Interesting "fun" features
**Priority: P2 — add after core workflows are reliable**
- [ ] **Fire activity replay**: animate event activity through a selected date range on the map, with play/pause and speed controls.
- [ ] **Year slider**: scrub through 2000–2026 and watch activity change.
- [ ] **Global fire calendar**: a calendar heatmap showing daily event volume.
- [ ] **Fire fingerprint**: a compact visual summary of one incident's duration, event density, FRP, and source mix.
- [ ] **Activity pulse**: a live-feeling dashboard animation when the date range changes (not fake real-time data).
- [ ] **Explore a random fire**: open a random valid fire record for discovery.
- [ ] **Record of the day**: feature an unusual record, with a transparent explanation of why it was selected.
- [ ] **Extreme events leaderboard**: rank fires by max FRP, duration, or event count, with metric definitions.
- [ ] **Regional season explorer**: compare monthly patterns across selected regions.
- [ ] **Shareable query snapshots**: encode filters in a URL so another person can open the same exploration.
- [ ] **Download a chart**: export chart images or the underlying aggregated data.
- [ ] **Story mode**: guided, annotated exploration of a notable time period or region, backed by actual query results.

Avoid presenting the dataset as live fire alerts unless the ingestion pipeline is updated frequently and freshness is explicitly shown.

### J. Data quality and transparency
**Priority: P1**
- [ ] Dataset coverage by year and source.
- [ ] Last build date / dataset version.
- [ ] Row counts for fires and events.
- [ ] Validation status from the build pipeline.
- [ ] Explain the event clustering rule (1.5 km / 4 h, capped to a UTC day).
- [ ] Explain the persistent-fire grouping logic and its limitations.
- [ ] Add a data glossary and methodology page.
- [ ] Show when a query returns no data versus when a source/API request failed.
- [ ] Add a visible note that satellite active-fire detections are not the same as a complete inventory of every ground fire.

## 4. Visual design system

### Visual direction
Keep the black/starfield identity as a subtle background treatment, but make the actual dashboard surfaces calmer and more legible.
- **Background**: near-black / charcoal, not pure black everywhere.
- **Panels**: slightly lighter charcoal with restrained borders.
- **Text**: high-contrast primary text and muted secondary labels.
- **Accent**: one vivid warm accent for fire intensity, plus a cool secondary accent for selection/interaction.
- **Semantic colors**: use consistent colors for source systems, but always pair color with text labels.
- **Typography**: one strong display face for page titles and a highly readable UI/data font for tables and numbers.
- **Spacing**: consistent 4/8px spacing scale.
- **Radius**: modest panel rounding; avoid excessive pill-shaped everything.
- **Motion**: short, purposeful transitions; respect reduced-motion preferences.

### Component inventory
- [ ] App shell: top bar, sidebar, workspace, inspector.
- [ ] KPI card with value, label, period, and comparison.
- [ ] Filter bar and advanced-filter drawer.
- [ ] Date-range picker with presets.
- [ ] Chart card with title, subtitle, tooltip, legend, and export.
- [ ] Map toolbar, legend, layer switcher, and selection state.
- [ ] Data grid with server-side controls.
- [ ] Fire detail inspector.
- [ ] Empty/loading/error states.
- [ ] Toasts for copy/export/save actions.
- [ ] Saved-query card.
- [ ] Methodology/help popover.

### Charts worth building

| Chart | What it answers | Suggested form |
| --- | --- | --- |
| Activity over time | When did activity rise or fall? | Line/area |
| Monthly seasonality | Which months are most active? | Bar |
| Fire vs event counts | Are incidents and observations moving together? | Dual series |
| FRP distribution | How intense are observations/incidents? | Histogram |
| Source contribution | Which sensors contributed? | Stacked bar |
| Duration distribution | How long do persistent incidents last? | Histogram/box plot |
| Daily activity calendar | Which days were unusually active? | Calendar heatmap |
| Regional comparison | Which selected area had more activity? | Grouped bar |
| Top incidents | Which records rank highest by a chosen metric? | Ranked horizontal bar |

Use histograms for distributions; do not use pie charts for many categories. Avoid dual axes unless the relationship is genuinely useful and clearly explained.

## 5. API and data work needed

The current API is a good foundation. The UI should not compensate for missing aggregations by downloading large result sets.

### Keep and reuse
- `GET /health`
- `GET /years`
- `GET /fires`
- `GET /fires/{fire_id}`
- `GET /fires/{fire_id}/events`
- `GET /events`
- `GET /events/{event_id}`

### Recommended API additions
- [ ] `GET /stats/overview?from=&to=&bbox=&...` — KPI aggregates for the active query.
- [ ] `GET /stats/timeseries?interval=day|week|month&...` — fire/event/FRP series.
- [ ] `GET /stats/sources?...` — source contribution aggregates.
- [ ] `GET /stats/distribution?metric=frp|duration|event_count&...` — histogram bins.
- [ ] `GET /stats/top-fires?metric=max_frp|duration|event_count&...` — ranked records.
- [ ] `GET /stats/grid?...` — spatial grid/hexbin aggregate for map rendering.
- [ ] `GET /regions` or a boundary-data service — only if administrative geography is implemented.
- [ ] `POST /exports` or a bounded export endpoint — generate CSV/GeoJSON without exposing object storage.
- [ ] Optional saved-query persistence if user accounts are introduced.

### For every endpoint
- Validate date, bbox, radius, and pagination parameters.
- Return consistent metadata: applied filters, result count, aggregation interval, and dataset version.
- Add response caching for repeatable aggregate queries.
- Add sensible maximum ranges/limits.
- Use DuckDB predicate and partition pushdown; verify query plans.
- Avoid expensive full scans on every map pan.
- Consider precomputed yearly/monthly or spatial aggregates if interactive analytics become slow.
- Keep API response schemas stable and documented.

### Important performance note
Parquet + DuckDB is excellent for analytical scans and this dataset size. It is not automatically an indexed point-lookup database. Keep the current analytical path for broad filters and aggregates. If exact-ID lookups or high-concurrency interactive traffic become a bottleneck, add a small indexed serving layer for IDs/metadata rather than replacing the analytical storage prematurely.

## 6. Suggested implementation phases

### Phase 0 — UX foundation and correctness
- [ ] Audit the existing frontend and map lifecycle.
- [ ] Fix the repeated-world map behavior (`noWrap`, bounds, one map instance).
- [ ] Establish the dashboard shell and responsive layout.
- [ ] Create reusable cards, filters, chart containers, and loading/error states.
- [ ] Define a single shared query state for date, year, bbox/radius, source, and metric.
- [ ] Ensure map, grid, charts, and detail panel always reflect the same query.

**Done when**: searching once updates the existing map, results, and summary without duplicate maps, stale panels, or inconsistent filters.

### Phase 1 — Core dashboard
- [ ] Build overview KPIs.
- [ ] Build activity-over-time chart.
- [ ] Build source contribution chart.
- [ ] Build the server-paginated fire grid.
- [ ] Build the fire detail inspector and event timeline.
- [ ] Add URL-shareable filter state.

**Done when**: a user can search a region and period, understand the summary, inspect the map, sort records, and open an incident's events.

### Phase 2 — Analytical features
- [ ] Add monthly seasonality and FRP distribution.
- [ ] Add top-fire rankings.
- [ ] Add spatial grid/hexbin aggregation.
- [ ] Add year-over-year and period comparison.
- [ ] Add source filters and sensor comparison.
- [ ] Add export for bounded results.

**Done when**: users can answer comparative questions without manually exporting data into another tool.

### Phase 3 — Signature experiences
- [ ] Add fire activity replay.
- [ ] Add global fire calendar.
- [ ] Add fire fingerprint.
- [ ] Add random-fire discovery and extreme-event leaderboard.
- [ ] Add story mode for selected events/regions.
- [ ] Add saved queries if there is a real need for persistence.

**Done when**: the product has memorable exploration features that are grounded in the underlying data, not just visual gimmicks.

### Phase 4 — Production hardening
- [ ] Test API latency and query plans with realistic concurrent use.
- [ ] Add caching and rate limits.
- [ ] Add observability for endpoint latency, errors, and query volume.
- [ ] Test keyboard navigation and screen-reader labels.
- [ ] Test mobile/tablet layouts.
- [ ] Test date-line, polar, empty, and very dense map queries.
- [ ] Add automated frontend and API integration tests.
- [ ] Document deployment, data versioning, and rebuild procedure.

## 7. Task backlog with priority

| ID | Task | Priority | Dependency |
| --- | --- | --- | --- |
| UI-01 | Build dashboard shell and responsive navigation | P0 | None |
| MAP-01 | Ensure one persistent Leaflet map instance | P0 | Frontend audit |
| MAP-02 | Disable repeated world tiles and constrain map bounds | P0 | MAP-01 |
| DATA-01 | Define shared filter/query state | P0 | UI-01 |
| API-01 | Add overview aggregate endpoint | P0 | Query definitions |
| API-02 | Add time-series aggregate endpoint | P0 | Query definitions |
| UI-02 | Add KPI cards and active-filter summary | P0 | API-01 |
| GRID-01 | Add server-side sortable fire grid | P0 | Existing `/fires` |
| DETAIL-01 | Add fire inspector and event timeline | P0 | Existing detail endpoints |
| CHART-01 | Add activity time-series | P0 | API-02 |
| CHART-02 | Add source contribution chart | P1 | Stats endpoint |
| API-03 | Add FRP/duration histogram endpoint | P1 | Query definitions |
| MAP-03 | Add clustering and metric-based marker styling | P1 | Spatial result strategy |
| API-04 | Add spatial grid/hexbin aggregation | P1 | Spatial query design |
| COMP-01 | Add date-range comparison | P1 | Aggregate endpoints |
| COMP-02 | Add geographic comparison | P1 | Spatial filtering |
| DATA-02 | Add dataset coverage and methodology view | P1 | Build metadata |
| EXPORT-01 | Add bounded CSV/GeoJSON export | P1 | Export design |
| FUN-01 | Add fire activity replay | P2 | Event timestamps |
| FUN-02 | Add global fire calendar | P2 | Daily aggregate endpoint |
| FUN-03 | Add fire fingerprint visualization | P2 | Fire detail data |
| FUN-04 | Add random-fire discovery | P2 | Fire lookup strategy |
| FUN-05 | Add story mode | P2 | Curated query snapshots |
| QA-01 | Add responsive, accessibility, and map edge-case tests | P0 | UI foundation |
| OPS-01 | Add API monitoring, caching, and rate limits | P1 | API deployment |

## 8. Suggested first release scope

Do not attempt to build every feature at once. A strong first release should contain:
1. A polished dashboard shell.
2. One persistent, non-repeating global map.
3. Search/filter controls with shareable query URLs.
4. KPI cards.
5. Activity-over-time chart.
6. Source contribution chart.
7. A server-side sortable fire grid.
8. Fire detail inspector with event timeline.
9. Clear methodology and data coverage information.

That is already a substantial and credible scientific data explorer. Comparison, heatmaps, replay, and story mode can follow.

## 9. Definition of done

The redesign is successful when:
- A user can understand global or filtered activity within a few seconds.
- Map, charts, KPIs, and grid use the same filters and definitions.
- The map appears only once and does not repeat horizontally.
- A user can go from a global overview → a region → a fire → its events without losing context.
- Large datasets remain server-side; browser responses stay bounded.
- Charts explain their aggregation and units.
- Empty, loading, and failure states are intentional.
- The interface works on desktop and mobile.
- The product is visually distinctive without sacrificing readability or analytical credibility.
