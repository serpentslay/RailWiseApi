type ResponsePanelProps = {
    responseData: unknown;
};

export default function ResponsePanel({ responseData }: ResponsePanelProps) {
    return (
        <section className="response-panel">
            <h2>API response</h2>
            <pre>{JSON.stringify(responseData, null, 2)}</pre>
        </section>
    );
}
