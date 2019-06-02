package com.keepgulp.taobaoschedulelearn.serverdefine;

import java.sql.Timestamp;
import java.util.Comparator;

public class ScheduleServerComparator implements Comparator<ScheduleServer> {

    String[] orderFields;

    public ScheduleServerComparator(String aOrderStr) {
        if (aOrderStr != null) {
            orderFields = aOrderStr.toUpperCase().split(",");
        } else {
            orderFields = "TASK_TYPE,OWN_SIGN,REGISTER_TIME,HEARTBEAT_TIME,IP".toUpperCase().split(",");
        }
    }

    public int compareObject(String o1, String o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 != null) {
            return o1.compareTo(o2);
        } else {
            return -1;
        }
    }

    public int compareObject(Timestamp o1, Timestamp o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 != null) {
            return o1.compareTo(o2);
        } else {
            return -1;
        }
    }

    @Override
    public int compare(ScheduleServer o1, ScheduleServer o2) {
        int result = 0;
        for (String name : orderFields) {
            if (name.equals("TASK_TYPE")) {
                result = compareObject(o1.getTaskType(), o2.getTaskType());
                if (result != 0) {
                    return result;
                }
            } else if (name.equals("OWN_SIGN")) {
                result = compareObject(o1.getOwnSign(), o2.getOwnSign());
                if (result != 0) {
                    return result;
                }
            } else if (name.equals("REGISTER_TIME")) {
                result = compareObject(o1.getRegisterTime(), o2.getRegisterTime());
                if (result != 0) {
                    return result;
                }
            } else if (name.equals("HEARTBEAT_TIME")) {
                result = compareObject(o1.getHeartBeatTime(), o2.getHeartBeatTime());
                if (result != 0) {
                    return result;
                }
            } else if (name.equals("IP")) {
                result = compareObject(o1.getIp(), o2.getIp());
                if (result != 0) {
                    return result;
                }
            } else if (name.equals("MANAGER_FACTORY")) {
                result = compareObject(o1.getManagerFactoryUUID(), o2.getManagerFactoryUUID());
                if (result != 0) {
                    return result;
                }
            }
        }
        return result;
    }
}

