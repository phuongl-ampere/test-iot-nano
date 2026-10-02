# Power Monitor YMS visual style

## Goal

Restyle the Power Monitor application with a light, compact YMS workspace
while preserving every existing route, control, interaction, and data state.

## Visual system

The stylesheet will declare these source tokens:

```css
--yms-canvas: #eef1f2;
--yms-panel: #ffffff;
--yms-primary: #253957;
--yms-accent: #ff775c;
--yms-radius: 9px;
```

Supporting semantic tokens will provide legible text, subtle blue-grey borders,
shadows, input surfaces, and the existing success, warning, and error states.
Existing `--pm-*` references will resolve through the new token system, keeping
component markup and behaviour unchanged.

## Layout and components

- The asset explorer remains a navy `#253957` rail. The selected resource gets
  a restrained coral indicator.
- The workspace uses `#eef1f2`; its content areas are white cards or panels with
  a 9px radius, a fine border, and a very light shadow.
- Dashboard sections, telemetry cards, forms, invitations, login, and the edit
  drawer receive the same surface treatment so the application reads as one
  system.
- Primary buttons use navy. Coral is reserved for focus outlines, active states,
  and compact selection cues. Online, warning, and error colours retain their
  established semantic roles.

## Typography and controls

- `h1`, `h2`, `h3`, and brand/display headings use `Iowan Old Style`, with
  Georgia and standard serif fallbacks.
- Body copy, fields, tables, and controls use the operating system sans-serif
  stack.
- Buttons and inputs use consistent heights, padding, radii, and alignment.
  Action groups wrap deliberately rather than overflow, maintaining a tidy row
  at wide widths and a usable flow on narrow screens.

## Responsive and accessible behaviour

- Desktop keeps the persistent rail and spacious content grid.
- Tablet action groups wrap while retaining aligned control heights.
- At the existing small-screen breakpoint, the rail becomes the top section and
  cards reduce padding; controls remain reachable without horizontal overflow.
- Keyboard focus uses a visible coral outline with adequate separation from
  nearby surfaces. No interaction logic or accessible labels changes.

## Validation

Add a focused stylesheet contract test before styling changes. It will fail
until the YMS token declarations, serif-heading rule, and responsive/control
rules are present. Then run the Power Monitor test and build commands, plus a
visual review at desktop and mobile widths when the application is runnable.

## Scope boundary

This is a presentation-only change to `apps/powermonitor`. It does not modify
API calls, React state, routes, business rules, or device telemetry behaviour.
