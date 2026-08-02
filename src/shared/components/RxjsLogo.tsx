/**
 * An RxJS-flavoured mark: an abstract marble diagram — a timeline with three
 * emitted "marbles" and a completion bar (`----●--●--●--|`). Marble diagrams are
 * RxJS's signature way of drawing a stream over time. Rendered in the current
 * text colour so callers control the accent.
 */
export function RxjsLogo({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      aria-hidden="true"
      focusable="false"
      className={className}
    >
      {/* the timeline */}
      <line x1="2.5" y1="12" x2="19.5" y2="12" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      {/* completion notification */}
      <line x1="21.4" y1="8.4" x2="21.4" y2="15.6" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      {/* emitted marbles */}
      <circle cx="6" cy="12" r="2.3" fill="currentColor" />
      <circle cx="12" cy="12" r="3" fill="currentColor" />
      <circle cx="18" cy="12" r="2.3" fill="currentColor" />
    </svg>
  );
}
