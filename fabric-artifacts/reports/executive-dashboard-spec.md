# Executive Dashboard Specification — AgriTech Analytics

## Overview

**Report Name:** AgriTech Analytics — Executive Dashboard
**Model:** AgriTech-Analytics (Direct Lake semantic model over Gold-layer Delta tables)
**Pages:** 3 (Operations Pulse, Yield Deep-Dive, Operational Excellence)
**Refresh:** Zero-copy via Direct Lake — no scheduled Import refresh needed

## Design System

### Color Palette
| Role | Hex | Usage |
|------|-----|-------|
| Primary | `#2D8B4E` | KPI card headers, primary buttons, positive trends |
| Dark | `#1A5C32` | Title bars, navigation, emphasis text |
| Light | `#7BC67E` | Sparkline fills, chart accents, secondary highlights |
| Background | `#F5F5F5` | Page canvas background |
| Surface | `#FFFFFF` | Card backgrounds |
| Text Primary | `#333333` | Body text, axis labels |
| Text Secondary | `#666666` | Subtitles, annotations |
| Danger | `#D32F2F` | Negative variances, critical alerts |
| Warning | `#F9A825` | Below-target indicators, moderate alerts |
| Neutral | `#90A4AE` | Inactive states, grid lines |

### Typography
- **Title:** Segoe UI Semibold 18pt (`#1A5C32`)
- **Subtitle:** Segoe UI Regular 11pt (`#666666`)
- **KPI Value:** Segoe UI Bold 28pt (`#333333`)
- **KPI Label:** Segoe UI Regular 10pt (`#666666`)
- **Card Header:** Segoe UI Semibold 12pt (`#1A5C32`)

### Layout Grid
- Canvas: 1280 × 720 px (16:9 widescreen)
- Top slicer bar: y=0, height=56 px
- Content area: y=56 to y=720
- Column grid: 4 columns, 16 px gutters
- Card padding: 12 px internal

---

## Global Slicer Bar (all pages)

**Position:** Top of every page, full width (0, 0, 1280, 56)
**Background:** `#1A5C32`
**Logo:** AgriTech Analytics leaf icon (left, 8 px padding)

| Slicer | Field | Type | Default |
|--------|-------|------|---------|
| Greenhouse | `dim_greenhouse[name]` | Dropdown | All |
| Date Range | `dim_date[date]` | Date range picker | Last 30 days |
| Crop | `dim_crop[display_name]` | Dropdown (multi) | All |
| Zone | `dim_zone[zone_display_name]` | Dropdown (multi) | All |

---

## Page 1: Operations Pulse

**Purpose:** Executive landing page — at-a-glance KPIs, YoY comparisons, greenhouse performance comparison. This is the first thing the VP of Operations sees every morning.

### Row 1 — KPI Cards (y=72, height=120)

Six KPI cards in a single row across the top. Each card shows the current value, a YoY delta, and a sparkline.

| Position | Card Title | Measure | Format | Conditional Color |
|----------|-----------|---------|--------|-------------------|
| Top-Left (col 1) | Total Yield | `Total Harvest (kg)` | `#,##0` kg | Green if YoY > 0, Red if < 0 |
| Col 2 | Grade A Quality | `Grade A %` | `0.0%` | Green ≥ 85%, Yellow ≥ 75%, Red < 75% |
| Col 3 | Energy Cost/Kg | `Energy Cost per Kg` | `$0.00` | Green if YoY improving, Red if worsening |
| Col 4 | On-Time Delivery | `On-Time Delivery %` | `0.0%` | Green ≥ 95%, Yellow ≥ 90%, Red < 90% |
| Col 5 | Anomaly Count | `Anomaly Count` | `#,##0` | Green ≤ 3, Yellow ≤ 6, Red > 6 |
| Top-Right (col 6) | Prevented Loss | `Prevented Loss (USD)` | `$#,##0` | Always `#2D8B4E` |

**Sparklines:** Below each KPI value, a 90-day area sparkline using the same measure trended by `dim_date[date]`. Fill: `#7BC67E` at 20% opacity.

**YoY Delta:** Below sparkline, show `↑ 4.2%` or `↓ 1.8%` using `YoY Yield Growth %` (or equivalent per measure). Green text for improvement, red for decline.

### Row 2 — Greenhouse Comparison (y=208, height=240)

| Position | Visual | Details |
|----------|--------|---------|
| Left Half (0–640) | **Clustered Bar Chart** — "Yield by Greenhouse & Crop" | X-axis: `dim_crop[display_name]`, Y-axis: `Total Harvest (kg)`, Legend: `dim_greenhouse[name]`. Colors: BrightHarvest = `#2D8B4E`, Mucci Valley = `#1A5C32`. Sort descending by total yield. |
| Right Half (640–1280) | **Line Chart** — "Monthly Yield Trend (YoY Overlay)" | X-axis: `dim_date[month_name]`, Y-axis: `Total Harvest (kg)`, Legend: `dim_date[year]`. Current year solid line `#2D8B4E`, prior year dashed `#90A4AE`. Show data labels on current year. |

### Row 3 — Zone Health Matrix (y=464, height=240)

| Position | Visual | Details |
|----------|--------|---------|
| Left Half (0–640) | **Matrix / Heatmap** — "Zone Health Scorecard" | Rows: `dim_zone[zone_display_name]`, Columns: `Avg Zone Temperature (°C)`, `Optimal Hours %`, `Avg DLI (mol/m²/day)`, `Grade A %`. Conditional formatting: cell background gradient from Red (`#D32F2F`) through Yellow (`#F9A825`) to Green (`#2D8B4E`). Row grouped by `dim_greenhouse[name]`. |
| Right Half (640–1280) | **Donut Charts (2)** — "Greenhouse Resource Split" | Two donut charts side by side. Left donut: Energy Cost split by greenhouse. Right donut: Cases Shipped split by greenhouse. Center label shows total value. Colors: `#2D8B4E` / `#1A5C32`. |

---

## Page 2: Yield Deep-Dive

**Purpose:** Detailed production analysis for the Head of Growing. Drill into yield by zone, understand environmental correlations, identify variance drivers.

### Row 1 — Zone Yield Matrix (y=72, height=200)

| Position | Visual | Details |
|----------|--------|---------|
| Full Width | **Matrix** — "Zone Production Summary" | Rows: `dim_greenhouse[name]` → `dim_zone[zone_display_name]` (expandable hierarchy). Columns: `Total Harvest (kg)`, `Yield per SqFt (kg)`, `Grade A %`, `Waste %`, `Avg Cycle Length (days)`, `Revenue (USD)`. Conditional formatting on each column with data bars (`#7BC67E`). Subtotals per greenhouse, grand total at bottom. |

### Row 2 — Scatter & Waterfall (y=288, height=220)

| Position | Visual | Details |
|----------|--------|---------|
| Left Half (0–640) | **Scatter Plot** — "DLI vs Yield Correlation" | X-axis: `Avg DLI (mol/m²/day)`, Y-axis: `Total Harvest (kg)`, Size: `dim_zone[size_sqft]`, Color: `dim_crop[display_name]`. Show trend line. Tooltip: zone name, crop, DLI, yield, optimal hours %. Demonstrates light → yield relationship. |
| Right Half (640–1280) | **Waterfall Chart** — "Yield Variance: This Month vs Last Month" | Categories: Each crop. Values: variance in `Total Harvest (kg)` between current and prior month. Positive bars: `#2D8B4E`. Negative bars: `#D32F2F`. Total bar: `#1A5C32`. Shows which crops drove overall production change. |

### Row 3 — Time Series & Distribution (y=524, height=180)

| Position | Visual | Details |
|----------|--------|---------|
| Left Half (0–640) | **Area Chart** — "Daily Yield Trend (last 90 days)" | X-axis: `dim_date[date]`, Y-axis: `Total Harvest (kg)`, Split by `dim_greenhouse[name]`. Stacked area. Colors: BrightHarvest `#7BC67E`, Mucci Valley `#2D8B4E`. Show 7-day moving average overlay line in `#1A5C32`. |
| Right Half (640–1280) | **100% Stacked Bar** — "Quality Grade Distribution by Crop" | X-axis: `dim_crop[display_name]`, Y-axis: percentage. Segments: Grade A (`#2D8B4E`), Grade B (`#F9A825`), Waste (`#D32F2F`). Data labels show percentages. Target line at 85% Grade A. |

---

## Page 3: Operational Excellence

**Purpose:** Sustainability, anomaly management, and ROI metrics for the CFO and sustainability team. Demonstrates the value of predictive operations.

### Row 1 — Sustainability Gauges (y=72, height=180)

Four gauge visuals in a row showing sustainability KPIs.

| Position | Gauge | Measure | Target | Format |
|----------|-------|---------|--------|--------|
| Col 1 | Water Recycling | `Water Recycling Rate %` | 85% | Percentage. Green ≥ 85%, Yellow ≥ 75%, Red < 75%. |
| Col 2 | Energy Efficiency | `YoY Energy Efficiency %` | 5% improvement | Percentage. Green ≥ 5%, Yellow ≥ 0%, Red < 0%. |
| Col 3 | Waste Reduction | `Waste %` (inverted) | < 5% waste | Percentage. Green ≤ 5%, Yellow ≤ 8%, Red > 8%. |
| Col 4 | Carbon Intensity | `Carbon Footprint (kg CO2e)` / `Total Harvest (kg)` | < 0.3 kg CO₂e/kg | Decimal. Green ≤ 0.3, Yellow ≤ 0.5, Red > 0.5. |

**Gauge design:** Semi-circle gauge. Arc fill matches conditional color. Center shows current value in bold. Subtitle shows target. Outer ring shows YoY direction arrow.

### Row 2 — Anomaly Timeline & Energy (y=268, height=220)

| Position | Visual | Details |
|----------|--------|---------|
| Left Half (0–640) | **Timeline Chart** — "Anomaly Events (last 12 months)" | X-axis: `dim_date[date]` (month granularity), Y-axis: `Anomaly Count`. Color by anomaly type: hvac_failure = `#D32F2F`, nutrient_drift = `#F9A825`, irrigation_failure = `#1565C0`, cold_chain_break = `#7B1FA2`, unknown = `#90A4AE`. Stacked column chart. Overlay line: `Avg Response Time (min)` on secondary axis in `#333333` dashed. |
| Right Half (640–1280) | **Combo Chart** — "Energy Cost vs Yield (Monthly)" | X-axis: `dim_date[month_year_label]`. Columns: `Energy Cost (USD)` in `#F9A825`. Line: `Total Harvest (kg)` in `#2D8B4E`. Secondary axis for yield. Shows the cost-to-production ratio visually. Target line for energy budget. |

### Row 3 — ROI Cards & Table (y=504, height=200)

| Position | Visual | Details |
|----------|--------|---------|
| Left Third (0–420) | **3 ROI KPI Cards** (stacked) | Card 1: "Prevented Losses" — `Prevented Loss (USD)` — large green number, subtitle "from early anomaly detection". Card 2: "Avg Response Time" — `Avg Response Time (min)` — value with target comparison "Target: < 15 min". Card 3: "Days Since Last Incident" — `Days Since Last Anomaly` — large number, green if > 14, yellow if > 7, red if ≤ 7. |
| Right Two-Thirds (420–1280) | **Table** — "Customer Delivery Scorecard" | Columns: `dim_customer[name]`, `Shipment Count`, `On-Time Delivery %`, `Cold Chain Compliance %`, `Fill Rate %`, `Avg Shelf Life (days)`. Conditional formatting: On-Time ≥ 95% green, ≥ 90% yellow, < 90% red. Cold Chain ≥ 98% green, ≥ 95% yellow, < 95% red. Sort by On-Time Delivery ascending (worst first). Row highlight on hover. |

---

## Drill-Through & Interactions

### Drill-Through Pages (hidden)
- **Zone Detail:** Drill from any zone reference → full zone environment history, equipment status, harvest log
- **Customer Detail:** Drill from customer name → shipment list, delivery map, compliance trend

### Cross-Filter Behavior
- Selecting a greenhouse in any visual filters all other visuals on the page
- Selecting a crop in Row 2 of Yield Deep-Dive filters the matrix in Row 1
- Date range slicer in the top bar applies globally across all pages

### Tooltips (custom)
- **Zone tooltip:** Zone name, crop, current temp vs setpoint, health score, last alert
- **Shipment tooltip:** Shipment ID, customer, product, departure time, ETA, temp status

---

## Bookmarks (for demo flow)

| Bookmark | Page | State | Purpose |
|----------|------|-------|---------|
| "Morning Overview" | Operations Pulse | Last 7 days, all greenhouses | Default landing state |
| "BrightHarvest Focus" | Operations Pulse | BrightHarvest only, last 30 days | Show single-facility view |
| "Yield Comparison" | Yield Deep-Dive | All greenhouses, last 90 days | Production analysis |
| "Sustainability Report" | Operational Excellence | All, last quarter | CFO sustainability review |
| "Anomaly Demo" | Operational Excellence | Last 24 hours | Show after triggering live anomaly |

---

## Mobile Layout

Simplified single-column layout for each page:
- KPI cards stack 2-wide
- Charts render full-width, reduced height
- Matrix switches to a summary card view
- Slicer bar collapses to a filter icon with slide-out panel
