import {SideBar} from "./components/SideBar/SideBar";
import {ChatBot} from "./components/ChatBot/ChatBot";
import './App.css';

function App() {
    return (
        <div className="App">
            <div className={"app__background"}>
                <img src={"pagina_INEGI.jpg"} alt={""}/>
            </div>
            <SideBar/>
            <ChatBot/>
        </div>
    );
}

export default App;
