import "./ChatBotMessageGraph.css";
import {AiOutlineExpandAlt} from "react-icons/ai";
import { IconContext } from "react-icons";
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

export const ChatBotMessageGraph = ({graph, setActiveGraph}) => {
    const options = {
        responsive: true,
        plugins: {
            legend: {
                display: false,
            },
            title: {
                display: true,
                text: 'Desgloce Poblacional',
            },
        },
    };

    const dataset_formatter = (graph) => {
        const datasets = [];

        for (let i = 0; i < graph.datasets.length; i++) {
            const dataset = {
                label: graph.datasets[i].label,
                data: graph.labels.map((label, index) => graph.datasets[i].data[index]),
                backgroundColor: graph.datasets[i].backgroundColor,
            };
            datasets.push(dataset);
        }

        return datasets;
    }

    const buttonClick = () => {
        setActiveGraph({data: data_formatted});
    }

    const data_formatted = {
        labels: graph.labels,
        datasets: dataset_formatter(graph)
            // [
            // {
            //     label: 'Población',
            //     data: graph.data,
            //     backgroundColor: 'rgb(255, 99, 132)'
            // }
        // ]
    };

    return (
        <div className={"chatbot__message__graph"}>
            <button className={"chatbot__message__graph__expand"} onClick={buttonClick}><IconContext.Provider value={{size:"1.2rem"}}><AiOutlineExpandAlt/></IconContext.Provider></button>
            <Bar options={options} data={data_formatted}/>
        </div>
    )
}