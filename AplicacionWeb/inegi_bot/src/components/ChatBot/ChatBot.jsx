import "./ChatBot.css";
import {IoSend} from "react-icons/io5";
import {SiChatbot} from "react-icons/si"
import {AiOutlineQuestionCircle} from "react-icons/ai";
import {IconContext} from "react-icons";
import {useState, createRef, useEffect} from "react";
import {ChatBotMessageGraph} from "../ChatBotMessageGraph/ChatBotMessageGraph";

export const ChatBot = ({setActiveGraph}) => {
    const [chatLog, setChatLog] = useState([{
        text: "Hola, soy INEGI BOT, ¿en qué te puedo ayudar?", type: "simple", user: false
    }]);
    const inputRef = createRef();
    const apiURL = "http://127.0.0.1:5000/query/";
    const [helpPanel, setHelpPanel] = useState(false);

    const inputEnterPress = (event) => {
        if (event.key === "Enter" && !chatLog[chatLog.length - 1].user) {
            sendMessage();
        }
    }

    const sendMessage = () => {
        const input = inputRef.current.value;
        if (input !== "") {
            setChatLog([...chatLog, {text: input, type: "simple", user: true}]);
            inputRef.current.value = "";
        }
    }


    useEffect(() => {
        const updateChatLog = (r) => {
            if (r.type === "simple") {
                setChatLog(currentChatLog => {
                    return [...currentChatLog, {text: r.message, type: "simple", user: false}]
                });
            } else if (r.type === "graph_1") {
                setChatLog(currentChatLog => {
                    return [...currentChatLog, {text: r.message, type: "simple", user: false}]
                });
                setChatLog(currentChatLog => {
                    return [...currentChatLog, {text: "", type: "graph_1", user: false, graph: r.graph}]
                });
            } else if (r.type === "graph_2") {
                setChatLog(currentChatLog => {
                    return [...currentChatLog, {text: r.message, type: "simple", user: false}]
                });
                setChatLog(currentChatLog => {
                    return [...currentChatLog, {text: "", type: "graph_2", user: false, graph: r.graph}]
                });
            }
        }


        const sendRequest = (input) => {
            fetch(apiURL + input, {
                method: "GET", headers: {
                    "Content-Type": "application/json"
                }
            })
                .then(response => response.json())
                .then(data => {
                    updateChatLog(data);
                })
                .catch(error => {
                    setChatLog(currentChatLog => {
                        return [...currentChatLog, {text: "Hubo un error al contactar el servicio del INEGIBot, por favor intente nuevamente mas tarde.", type: "simple", user: false}]
                    });
                });
        }

        const chatBot = document.querySelector(".chatbot__body__container__message");
        chatBot.scrollTop = chatBot.scrollHeight;
        if (chatLog[chatLog.length - 1].user === true) {
            sendRequest(chatLog[chatLog.length - 1].text);
        }
    }, [chatLog]);

    return (
        <div className={"chatbot"}>
            <div className={"chatbot__container"}>
                <div className={"chatbot__header"}>
                    <h3>INEGI BOT</h3>
                    <IconContext.Provider value={{size:"1.65rem"}}>
                        <button className={"chatbot__header__help"} onClick={() => {setHelpPanel(current => {return !current})}}> {helpPanel ? <SiChatbot/> : <AiOutlineQuestionCircle/>}</button>
                    </IconContext.Provider>
                </div>
                <div className={"chatbot__body"}>
                    <div className={"chatbot__body__container"}>
                        {
                            helpPanel ?
                                <div className={"chatbot__body__container__help"}>
                                    <h3>Consultas disponibles</h3>
                                    <p>
                                        El INEGI BOT puede contestar preguntas acerca de la población de cualquier estado o municipio de Mexico.
                                        <br/>
                                        <br/>
                                        <h3> Las consultas utilizan 4 campos: </h3>
                                        <b>Estado:</b> El estado de Mexico del cual se quiere obtener informacion.
                                        <br/>
                                        <br/>
                                        <b>Municipio:</b> El municipio de Mexico del cual se quiere obtener informacion.
                                        <br/>
                                        <br/>
                                        <b>Filtro:</b> El filtro que se quiere aplicar a la consulta.
                                        Este puede ser uno de los siguientes:
                                        <ul>
                                            <li><b>Masculina:</b> Población masculina.</li>
                                            <li><b>Femenina:</b> Población femenina.</li>
                                            <li><b>Infantil:</b> Población infantil.</li>
                                            <li><b>Juvenil:</b> Población juvenil.</li>
                                            <li><b>Adulta:</b> Población adulta.</li>
                                            <li><b>Tercera edad:</b> Población mayor.</li>
                                        </ul>
                                        <b>Desglose:</b> El desglose que se quiere aplicar a la consulta.
                                        Este puede ser uno de los siguientes:
                                        <ul>
                                            <li><b>Edad:</b></li> Desglose por edad.
                                            <li><b>Sexo:</b></li> Desglose por sexo.
                                        </ul>

                                        A continuación se muestran ejemplos de consultas que puedes realizar al INEGI BOT:
                                    </p>
                                    <ul>
                                        <li><b>Población general:</b></li>
                                        ¿Cual es la población de "Ciudad", "Estado"?
                                        <li><b>Población con filtro de edad o sexo:</b></li>
                                        ¿Cual es la población "Filtro" de "Ciudad", "Estado"?
                                        <li><b>Población con desglose de edad o sexo:</b></li>
                                        ¿Cual es la población de "Estado" desglosada por "Desglose"?
                                        <li><b>Población con filtro de edad y desglose de sexo:</b></li>
                                        ¿Cual es la población "Filtro" de "Estado" desglosada por "Desglose"?
                                    </ul>

                                    Nota: para consultar la población de un municipio, se debe de tambien especificar el estado al que pertenece.
                                </div>
                            :
                                <>
                                    <div className={"chatbot__body__container__message"}>
                                        {
                                            chatLog.map((message, index) => {
                                                return (message.type === "simple" ?
                                                        <p className={message.user ? "chatbot__message__user" : "chatbot__message__bot"}
                                                           key={"message_"+index}>{message.text}</p>
                                                        : message.type === "graph_1" ?
                                                                <ChatBotMessageGraph key={"graph_"+index} graph={message.graph} setActiveGraph={setActiveGraph}/>
                                                            : message.type === "graph_2" ?
                                                                    <ChatBotMessageGraph key={"graph_"+index} graph={message.graph} setActiveGraph={setActiveGraph}/>
                                                                : {}

                                                )
                                            })
                                        }
                                    </div>
                                    <div className={"chatbot__body__container__input"}>
                                        <input ref={inputRef} type="text" placeholder={"Escribe tu mensaje"}
                                               onKeyDown={inputEnterPress}/>
                                        <button disabled={chatLog[chatLog.length - 1].user} onClick={sendMessage}><IoSend/>
                                        </button>
                                    </div>
                                </>
                        }
                    </div>
                </div>
            </div>
        </div>
    )
}