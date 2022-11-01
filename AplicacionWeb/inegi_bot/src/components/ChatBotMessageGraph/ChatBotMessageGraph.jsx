import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend,
} from 'chart.js';
import { Bar } from 'react-chartjs-2';

ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend
);

export const ChatBotMessageGraph = ({graph}) => {

    const options = {
        responsive: true,
        plugins: {
            legend: {
                position: 'top',
            },
            title: {
                display: true,
                text: 'Chart.js Bar Chart',
            },
        },
    };

    const data_formatted = {
        labels: graph.labels,
        datasets: [
            {
                label: 'Población',
                data: graph.data,
                backgroundColor: 'rgb(255, 99, 132)'
            }
        ]
    };

    return (
        <>
            <Bar options={options} data={data_formatted}/>
        </>
    )
}