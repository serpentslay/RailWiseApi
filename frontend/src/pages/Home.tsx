export default function Home() {
    return (
        <div style={{
            padding: "40px",
            fontFamily: "Arial"
        }}>
            <h1>My React SPA is working!!!!!!!!</h1>

            <p>
                This page is rendered by React inside IntelliJ.
            </p>

            <button onClick={() => alert("Button works!")}>
                Click me
            </button>
        </div>
    );
}