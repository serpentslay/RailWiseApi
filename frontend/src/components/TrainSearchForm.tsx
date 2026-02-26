import StationSelect from "./StationSelect";
import type { StationOption } from "../data/stations";

type TrainSearchFormProps = {
    fromStation: string;
    destinationStation: string;
    travelDate: string;
    arriveByTime: string;
    isLoading: boolean;
    stationOptions: StationOption[];
    onFromStationChange: (value: string) => void;
    onDestinationStationChange: (value: string) => void;
    onTravelDateChange: (value: string) => void;
    onArriveByTimeChange: (value: string) => void;
    onSubmit: () => void;
};

export default function TrainSearchForm({
    fromStation,
    destinationStation,
    travelDate,
    arriveByTime,
    isLoading,
    stationOptions,
    onFromStationChange,
    onDestinationStationChange,
    onTravelDateChange,
    onArriveByTimeChange,
    onSubmit,
}: TrainSearchFormProps) {
    return (
        <form
            className="planner-form"
            onSubmit={(event) => {
                event.preventDefault();
                onSubmit();
            }}
        >
            <StationSelect
                id="from-station"
                label="From station"
                value={fromStation}
                options={stationOptions}
                onChange={onFromStationChange}
            />

            <StationSelect
                id="destination-station"
                label="Destination station"
                value={destinationStation}
                options={stationOptions}
                onChange={onDestinationStationChange}
            />

            <label htmlFor="travel-date">
                Date
                <input
                    id="travel-date"
                    required
                    type="date"
                    value={travelDate}
                    onChange={(event) => onTravelDateChange(event.target.value)}
                />
            </label>

            <label htmlFor="arrive-by-time">
                Arrive by time
                <input
                    id="arrive-by-time"
                    required
                    type="time"
                    value={arriveByTime}
                    onChange={(event) => onArriveByTimeChange(event.target.value)}
                />
            </label>

            <button disabled={isLoading} type="submit">
                {isLoading ? "Loading..." : "Search"}
            </button>
        </form>
    );
}
