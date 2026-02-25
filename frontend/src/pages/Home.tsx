import { useMemo, useState } from "react";
import type { FormEvent } from "react";

type ReliabilityResponse = unknown;

const apiBaseUrl = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.replace(/\/$/, "") ?? "";

export default function Home() {
    const today = useMemo(() => new Date().toISOString().split("T")[0], []);
    const [fromStation, setFromStation] = useState("");
    const [destinationStation, setDestinationStation] = useState("");
    const [travelDate, setTravelDate] = useState(today);
    const [arriveByTime, setArriveByTime] = useState("09:00");
    const [responseData, setResponseData] = useState<ReliabilityResponse | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(false);

    const fetchReliability = async (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        setError(null);
        setResponseData(null);

        const params = new URLSearchParams({
            from_loc: fromStation.trim().toUpperCase(),
            to_loc: destinationStation.trim().toUpperCase(),
            date_str: travelDate,
            arrive_by: arriveByTime,
        });

        const endpoint = `${apiBaseUrl}/v1/reliability?${params.toString()}`;

        try {
            setIsLoading(true);
            const response = await fetch(endpoint, { method: "GET" });

            if (!response.ok) {
                const responseText = await response.text();
                throw new Error(responseText || `Request failed with status ${response.status}`);
            }

            const data: ReliabilityResponse = await response.json();
            setResponseData(data);
        } catch (requestError) {
            const message = requestError instanceof Error ? requestError.message : "An unknown error occurred.";
            setError(message);
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <main className="planner-page">
            <h1>Train selector</h1>
            <form className="planner-form" onSubmit={fetchReliability}>
                <label>
                    From station (CRS)
                    <input
                        required
                        minLength={3}
                        maxLength={3}
                        placeholder="PAD"
                        value={fromStation}
                        onChange={(event) => setFromStation(event.target.value)}
                    />
                </label>

                <label>
                    Destination station (CRS)
                    <input
                        required
                        minLength={3}
                        maxLength={3}
                        placeholder="RDG"
                        value={destinationStation}
                        onChange={(event) => setDestinationStation(event.target.value)}
                    />
                </label>

                <label>
                    Date
                    <input
                        required
                        type="date"
                        value={travelDate}
                        onChange={(event) => setTravelDate(event.target.value)}
                    />
                </label>

                <label>
                    Arrive by time
                    <input
                        required
                        type="time"
                        value={arriveByTime}
                        onChange={(event) => setArriveByTime(event.target.value)}
                    />
                </label>

                <button disabled={isLoading} type="submit">
                    {isLoading ? "Loading..." : "Search"}
                </button>
            </form>

            {error ? <p className="error-message">Error: {error}</p> : null}

            {responseData !== null ? (
                <section className="response-panel">
                    <h2>API response</h2>
                    <pre>{JSON.stringify(responseData, null, 2)}</pre>
                </section>
            ) : null}
        </main>
    );
}
