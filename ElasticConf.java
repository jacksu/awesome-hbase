package cn.thinkjoy.etl.elastic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhaochanghong on 15/8/28.
 */
public class ElasticConf implements Serializable{
    private String clusterName;
    private List<String> ipAndPorts = new ArrayList<String>();

    public ElasticConf(String clusterName,String ...ipAndPorts){
        this.clusterName = clusterName;
        for(String ipAndPort : ipAndPorts){
            this.ipAndPorts.add(ipAndPort);
        }
    }

    public void addIpAndPort(String ipAndPort) {
        this.ipAndPorts.add(ipAndPort);
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<String> getIpAndPorts() {
        return ipAndPorts;
    }

    public void setIpAndPorts(List<String> ipAndPorts) {
        this.ipAndPorts = ipAndPorts;
    }
}
