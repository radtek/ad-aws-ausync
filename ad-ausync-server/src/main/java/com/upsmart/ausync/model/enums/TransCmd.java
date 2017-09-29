package com.upsmart.ausync.model.enums;

/**
 * Created by yuhang on 17-9-28.
 */
public enum TransCmd {

    UNKNOWN("unknown"),

    AUDIENCE_UPDATE("AUDIENCE_UPDATE"),
    AUDIENCE_QUERY("AUDIENCE_QUERY");

    private String value;
    TransCmd(String value){
        this.value = value;
    }
    public String getValue(){
        return value;
    }
    public static TransCmd convert(String value) {
        for(TransCmd status : TransCmd.values()){
            if(status.getValue().equals(value)){
                return status;
            }
        }
        return UNKNOWN;
    }
}
