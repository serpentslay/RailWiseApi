import { useMemo, useState } from "react";
import ResponsePanel from "../components/ResponsePanel";
import TrainSearchForm from "../components/TrainSearchForm";
import { stations } from "../data/stations";

const apiBaseUrl = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.replace(/\/$/, "") ?? "";

function isJsonResponse(contentType: string | null): boolean {
    return contentType?.toLowerCase().includes("application/json") ?? false;
}

export default function Home() {
    const today = useMemo(() => new Date().toISOString().split("T")[0], []);
    const [fromStation, setFromStation] = useState("");
    const [destinationStation, setDestinationStation] = useState("");
    const [travelDate, setTravelDate] = useState(today);
    const [arriveByTime, setArriveByTime] = useState("09:00");
    const [responseData, setResponseData] = useState<unknown | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(false);

    const fetchReliability = async () => {
        setError(null);
        setResponseData(null);

        const params = new URLSearchParams({
            from_loc: fromStation,
            to_loc: destinationStation,
            date_str: travelDate,
            arrive_by: arriveByTime,
        });

        const endpoint = `${apiBaseUrl}/v1/reliability?${params.toString()}`;

        try {
            setIsLoading(true);
            const response = await fetch(endpoint, { method: "GET" });
            const contentType = response.headers.get("content-type");

            if (!response.ok) {
                const responseText = await response.text();
                throw new Error(responseText || `Request failed with status ${response.status}`);
            }

            if (!isJsonResponse(contentType)) {
                const responsePreview = (await response.text()).slice(0, 120);
                throw new Error(
                    `Expected JSON but received '${contentType ?? "unknown"}'. ` +
                        `Check VITE_API_BASE_URL or the Vite /v1 proxy. Response started with: ${responsePreview}`,
                );
            }

            const data: unknown = await response.json();
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

            <TrainSearchForm
                fromStation={fromStation}
                destinationStation={destinationStation}
                travelDate={travelDate}
                arriveByTime={arriveByTime}
                isLoading={isLoading}
                stationOptions={stations}
                onFromStationChange={setFromStation}
                onDestinationStationChange={setDestinationStation}
                onTravelDateChange={setTravelDate}
                onArriveByTimeChange={setArriveByTime}
                onSubmit={fetchReliability}
            />

            {error ? <p className="error-message">Error: {error}</p> : null}
            {responseData !== null ? <ResponsePanel responseData={responseData} requestedArriveBy={arriveByTime} /> : null}
        </main>
    );
}
