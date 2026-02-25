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

function formatDepartureTime(isoDateTime: string): string {
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

export default function ResponsePanel({ responseData }: ResponsePanelProps) {
    const results = Array.isArray(responseData) ? responseData.filter(isReliabilityResult) : [];

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
                {results.map((train) => (
                    <article className="result-card" key={`${train.departure_time}-${train.operator ?? "unknown"}`}>
                        <h3>{formatDepartureTime(train.departure_time)}</h3>
                        <p>
                            <strong>Operator:</strong> {train.operator ?? "Unknown"}
                        </p>
                        <p>
                            <strong>Reliability score:</strong> {train.reliability_score}
                        </p>
                        <p>
                            <strong>Confidence band:</strong> {train.confidence_band}
                        </p>
                    </article>
                ))}
            </div>
        </section>
    );
}
