import"./ChatBotButton.css";
import {SiChatbot} from "react-icons/si"

export const ChatBotButton = () => {

    function toggleChatBot() {
        const chatBot = document.querySelector(".chatbot");
        chatBot.classList.toggle("chatbot__active");

        const sideBar = document.querySelector(".sidebar");
        sideBar.classList.toggle("sidebar__active");
    }

    return (
        <>
            <div onClick={toggleChatBot}>
                <div className={"sidebar__btn btn__color-blue-4"}>INEGI BOT <SiChatbot/></div>
            </div>
        </>
    )
}