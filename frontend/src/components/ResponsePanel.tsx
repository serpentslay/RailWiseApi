import { useMemo, useState } from "react";

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

function trainId(train: ReliabilityResult): string {
    return `${train.departure_time}-${train.operator ?? "unknown"}`;
}

function getRecommendedTrains(results: ReliabilityResult[], currentTrain: ReliabilityResult): ReliabilityResult[] {
    return results
        .filter((train) => trainId(train) !== trainId(currentTrain))
        .slice()
        .sort((a, b) => b.reliability_score - a.reliability_score)
        .slice(0, 3);
}

export default function ResponsePanel({ responseData }: ResponsePanelProps) {
    const results = useMemo(
        () =>
            Array.isArray(responseData)
                ? responseData
                      .filter(isReliabilityResult)
                      .slice()
                      .sort((a, b) => new Date(b.departure_time).getTime() - new Date(a.departure_time).getTime())
                : [],
        [responseData],
    );

    const [selectedTrainId, setSelectedTrainId] = useState<string | null>(null);

    if (results.length === 0) {
        return (
            <section className="response-panel">
                <h2>Train results</h2>
                <p className="empty-results">No train results were returned for this search.</p>
            </section>
        );
    }

    const resolvedSelectedTrainId =
        selectedTrainId !== null && results.some((train) => trainId(train) === selectedTrainId)
            ? selectedTrainId
            : trainId(results[0]);

    return (
        <section className="response-panel">
            <h2>Train results</h2>
            <div className="results-grid">
                {results.map((train) => {
                    const isSelected = trainId(train) === resolvedSelectedTrainId;
                    const recommendations = isSelected ? getRecommendedTrains(results, train) : [];

                    return (
                        <article
                            className={`result-card ${isSelected ? "selected-card selected-card-large" : ""}`.trim()}
                            key={trainId(train)}
                        >
                            <div className="card-top-row">
                                <h3>{formatDateTime(train.departure_time)}</h3>
                                {isSelected ? <span className="selected-pill">Selected</span> : null}
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

                            {isSelected ? (
                                <div className="recommendations-block">
                                    <p className="recommendations-title">Recommended alternatives</p>
                                    {recommendations.length > 0 ? (
                                        <ul className="recommendations-list">
                                            {recommendations.map((recommended) => (
                                                <li key={`${trainId(recommended)}-alt`}>
                                                    <span>{formatDateTime(recommended.departure_time)}</span>
                                                    <span>{recommended.reliability_score}/100</span>
                                                    <button
                                                        className="switch-train-button"
                                                        onClick={() => setSelectedTrainId(trainId(recommended))}
                                                        type="button"
                                                    >
                                                        Switch
                                                    </button>
                                                </li>
                                            ))}
                                        </ul>
                                    ) : (
                                        <p className="recommendations-empty">No alternative trains returned.</p>
                                    )}
                                </div>
                            ) : null}
                        </article>
                    );
                })}
            </div>
        </section>
    );
}
