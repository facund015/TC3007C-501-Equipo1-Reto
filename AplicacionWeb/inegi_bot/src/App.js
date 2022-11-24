import {SideBar} from "./components/SideBar/SideBar";
import {ChatBot} from "./components/ChatBot/ChatBot";
import {GraphViewer} from "./components/GraphViewer/GraphViewer";
import './App.css';
import {useState} from "react";



function App() {
    const [active_graph, setActiveGraph] = useState(null);

    return (
        <div className="App">
            <div className={"app__background"}>
                <img src={"pagina_INEGI.jpg"} alt={""}/>
            </div>
            <SideBar/>
            <ChatBot setActiveGraph={setActiveGraph}/>
            <GraphViewer active_graph={active_graph} setActiveGraph={setActiveGraph}/>
        </div>
    );
}

export default App;
