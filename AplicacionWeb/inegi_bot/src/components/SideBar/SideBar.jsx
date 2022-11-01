import "./SideBar.css";
import {ChatBotButton} from "../ChatBotButton/ChatBotButton";
import {BsFillShareFill} from "react-icons/bs";
import {IoChatbubblesSharp} from "react-icons/io5";
import {HiOutlinePencilSquare} from "react-icons/hi2";
import { IconContext } from "react-icons";

export const SideBar = () => {
    return (
        <>
        <div className={"sidebar"}>
            <div className={"sidebar__container"}>
                <IconContext.Provider value={{size:"1.5rem", style: { rotate:"-180deg", transform:"translateX(0.35rem)"} }}>
                    <div className={"sidebar__btn btn__color-blue-1"}>Compartir <BsFillShareFill/></div>
                    <div className={"sidebar__btn btn__color-blue-2"}>Chat <IoChatbubblesSharp/></div>
                    <div className={"sidebar__btn btn__color-blue-3"}>Sugerencias <HiOutlinePencilSquare/></div>
                    <ChatBotButton/>
                </IconContext.Provider>
            </div>
        </div>
        </>
    )
}