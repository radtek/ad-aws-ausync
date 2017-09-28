package com.upsmart.ausync.model.enums;

/**
 * Created by yuhang on 17-9-28.
 */
public enum ActionType {
    UNKNOWN("unknown"),

    UPDATE("update");

    private String value;
    ActionType(String value){
        this.value = value;
    }
    public String getValue(){
        return value;
    }
    public static ActionType convert(String value) {
        for(ActionType status : ActionType.values()){
            if(status.getValue().equals(value)){
                return status;
            }
        }
        return UNKNOWN;
    }
}
