import type { StationOption } from "../data/stations";

type StationSelectProps = {
    id: string;
    label: string;
    value: string;
    options: StationOption[];
    onChange: (nextValue: string) => void;
};

export default function StationSelect({ id, label, value, options, onChange }: StationSelectProps) {
    return (
        <label htmlFor={id}>
            {label}
            <select id={id} required value={value} onChange={(event) => onChange(event.target.value)}>
                <option value="" disabled>
                    Select a station
                </option>
                {options.map((station) => (
                    <option key={station.code} value={station.code}>
                        {station.name}
                    </option>
                ))}
            </select>
        </label>
    );
}
