import "./GraphViewer.css"
import { Bar } from 'react-chartjs-2';
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend,
} from 'chart.js';

ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend
);

export const GraphViewer = ({active_graph, setActiveGraph}) => {
    const options = {
        responsive: true,
        plugins: {
            legend: {
                position: 'top',
            },
            title: {
                display: true,
                text: 'Desgloce Poblacional',
            },
        },
    };

    return (
        <>
            {active_graph === undefined || active_graph === null ? (
                <></>
            ) : (
                <div className={"graph-viewer__container"}>
                    <div className={"graph-viewer__background"}>
                        <div className={"graph-viewer__content"}>
                            <Bar options={options} data={active_graph.data} ></Bar>
                        </div>
                        <button className={"graph-viewer__close-button"} onClick={() => {setActiveGraph(null)}}>X</button>
                    </div>
                </div>
            )
            }

        </>
    )
}