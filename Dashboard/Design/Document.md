# Tiki E-commerce Platform Dashboard Design Document

## Project Overview
- **Project Name**: Dashboard for Tiki - E-commerce Platform
- **Project Description**: Creation of an administrative dashboard for Tiki, an e-commerce platform.

## Color Scheme

### Main Colors
- Primary Background: #FFFFFF
- Header Background: #0A68FF
- Highlight Color: #FFB600
- Text Color: #000000
- Container Background: #F8F8F8
- Button Background: #E6E6E6

### Chart Colors
1. Heatmap Color Palette:
   - Level 1: #0096C7
   - Level 2: #00B4D8
   - Level 3: #48CAE4
   - Level 4: #90E0EF
   - Level 5: #ADE8F4

2. KPI Color Palette:
   - Increase: #6FB47A
   - Decrease: #ED4D53

## Typography

### Font Family
- DIN (for all text elements)

### Font Styles and Sizes
- Header: Bold, 24px
- Subheader: Bold, 12px
- Regular Text: Regular, 10px

## UI Elements

### Buttons
- Background Color: #E6E6E6
- Border Radius: 30px
- Shape: Capsule (fully rounded ends)

### Chart Containers
- Background Color: #F8F8F8
- Border Radius: 12px
- Shape: Capsule (rounded rectangle)

## Layout Guidelines
[This section would typically include information about the overall layout structure, grid system, and spacing guidelines. As this information wasn't provided in the original content, it should be defined and added here.]

## Responsive Design
- Enable responsive interactions:
  - Allow users to click on each chart to expand or focus on its details
  - Implement collapsible filters that can be easily accessed and modified on all device sizes

## Interaction Design
- Implement hover effects for interactive elements:
  - Buttons: Slight color change and subtle shadow on hover
  - Chart elements: Highlight data points or segments on hover
- Add tooltip functionality to display additional information on hover for complex chart elements
- Enable chart expansion:
  - Allow users to click on charts to view an expanded version with more detailed information
  - Provide an option to view charts in full-screen mode
- Implement smooth transitions (0.3s duration) for all state changes
- For truncated text (indicated by "..."):
  - On hover, show the full text in a tooltip
  - For charts or statistic numbers, clicking the ellipsis should expand the element to show full details
- Add subtle animations for loading states and data updates to improve perceived performance
- Implement drag-and-drop functionality for customizable dashboard layouts (if applicable)

## Additional Notes
- The design should maintain a clean, professional look consistent with Tiki's brand identity.
- Charts and data visualizations should be clear and easy to interpret.
- The user interface should prioritize ease of use for administrators managing the e-commerce platform.