import "./ChatBot.css";
import {IoSend} from "react-icons/io5";
import {useState, createRef, useEffect} from "react";
import {ChatBotMessageGraph} from "../ChatBotMessageGraph/ChatBotMessageGraph";

export const ChatBot = () => {
    const [chatLog, setChatLog] = useState([{
        text: "Hola, soy INEGI BOT, ¿en qué te puedo ayudar?", type: "simple", user: false
    }]);
    const inputRef = createRef();
    const apiURL = "http://localhost:5000/message/";

    const inputEnterPress = (event) => {
        if (event.key === "Enter") {
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
                    console.log(data);
                    updateChatLog(data);
                })
                .catch(error => {
                    console.log(error);
                });
        }

        const chatBot = document.querySelector(".chatbot__body__container__message");
        chatBot.scrollTop = chatBot.scrollHeight;
        if (chatLog[chatLog.length - 1].user === true) {
            sendRequest(chatLog[chatLog.length - 1].text);
        }
    }, [chatLog]);

    return (<>
        <div className={"chatbot"}>
            <div className={"chatbot__container"}>
                <div className={"chatbot__header"}>
                    <h3>INEGI BOT</h3>
                </div>
                <div className={"chatbot__body"}>
                    <div className={"chatbot__body__container"}>
                        <div className={"chatbot__body__container__message"}>
                            {/*<p className={"chatbot__message__bot"}>Hola, soy INEGI BOT, ¿en qué te puedo ayudar?</p>*/}
                            {/*<p className={"chatbot__message__user"}>Cual es la poblacion de Monterrey?</p>*/}
                            {/*<p className={"chatbot__message__bot"}>En 2020 la poblacion de Monterrey era de 5,784,442 personas</p>*/}
                            {/*<p className={"chatbot__message__user"}>Cual es la poblacion de mujeres en Monterrey?</p>*/}
                            {/*<p className={"chatbot__message__bot"}>En 2020 la poblacion de mujeres en Monterrey era de 2,893,492</p>*/}
                            {chatLog.map((message, index) => {
                                return (message.type === "simple" ?
                                        <p className={message.user ? "chatbot__message__user" : "chatbot__message__bot"}
                                           key={"message_"+index}>{message.text}</p>
                                        : message.type === "graph_1" ?
                                                <ChatBotMessageGraph key={"graph_"+index} graph={message.graph}/>
                                            : message.type === "graph_2" ?
                                                    <ChatBotMessageGraph key={"graph_"+index} graph={message.graph}/>
                                                : {}

                                )
                            })}
                        </div>
                        <div className={"chatbot__body__container__input"}>
                            <input ref={inputRef} type="text" placeholder={"Escribe tu mensaje"}
                                   onKeyDown={inputEnterPress}/>
                            <button disabled={chatLog[chatLog.length - 1].user} onClick={sendMessage}><IoSend/>
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </>)
}