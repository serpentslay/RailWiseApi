type ReliabilityResult = {
    departure_time: string;
    operator: string | null;
    reliability_score: number;
    confidence_band: string;
};

type ResponsePanelProps = {
    responseData: unknown;
};

function isReliabilityResult(value: unknown): value is ReliabilityResult {
    if (!value || typeof value !== "object") {
        return false;
    }

    const candidate = value as Partial<ReliabilityResult>;
    return (
        typeof candidate.departure_time === "string" &&
        (typeof candidate.operator === "string" || candidate.operator === null) &&
        typeof candidate.reliability_score === "number" &&
        typeof candidate.confidence_band === "string"
    );
}

function formatDateTime(isoDateTime: string): string {
    const date = new Date(isoDateTime);

    if (Number.isNaN(date.getTime())) {
        return isoDateTime;
    }

    return date.toLocaleString(undefined, {
        hour: "2-digit",
        minute: "2-digit",
        day: "2-digit",
        month: "short",
        year: "numeric",
    });
}

function getReliabilityLabel(score: number): string {
    if (score >= 80) {
        return "High reliability";
    }
    if (score >= 60) {
        return "Moderate reliability";
    }
    return "Low reliability";
}

function getConfidenceClass(confidenceBand: string): string {
    const normalized = confidenceBand.toLowerCase();
    if (normalized === "high") {
        return "confidence-pill confidence-high";
    }
    if (normalized === "medium") {
        return "confidence-pill confidence-medium";
    }
    return "confidence-pill confidence-low";
}

export default function ResponsePanel({ responseData }: ResponsePanelProps) {
    const results = Array.isArray(responseData)
        ? responseData
              .filter(isReliabilityResult)
              .slice()
              .sort((a, b) => new Date(b.departure_time).getTime() - new Date(a.departure_time).getTime())
        : [];

    if (results.length === 0) {
        return (
            <section className="response-panel">
                <h2>Train results</h2>
                <p className="empty-results">No train results were returned for this search.</p>
            </section>
        );
    }

    return (
        <section className="response-panel">
            <h2>Train results</h2>
            <div className="results-grid">
                {results.map((train, index) => (
                    <article
                        className={`result-card ${index === 0 ? "selected-card" : ""}`.trim()}
                        key={`${train.departure_time}-${train.operator ?? "unknown"}`}
                    >
                        <div className="card-top-row">
                            <h3>{formatDateTime(train.departure_time)}</h3>
                            {index === 0 ? <span className="selected-pill">Selected</span> : null}
                        </div>

                        <p className="reliability-label">{getReliabilityLabel(train.reliability_score)}</p>

                        <div className="score-row">
                            <strong>Reliability score</strong>
                            <span>{train.reliability_score}/100</span>
                        </div>
                        <div className="score-bar" role="presentation">
                            <div className="score-fill" style={{ width: `${Math.max(0, Math.min(100, train.reliability_score))}%` }} />
                        </div>

                        <p>
                            <strong>Operator:</strong> {train.operator ?? "Unknown"}
                        </p>
                        <p>
                            <strong>Confidence band:</strong>{" "}
                            <span className={getConfidenceClass(train.confidence_band)}>{train.confidence_band}</span>
                        </p>
                    </article>
                ))}
            </div>
        </section>
    );
}
