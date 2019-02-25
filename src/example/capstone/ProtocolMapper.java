package example.capstone;

import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.core.Node;
import peersim.edsim.EDProtocol;

import static example.capstone.ProtocolMapperInit.*;

public class ProtocolMapper implements CDProtocol, EDProtocol {

    private final int brokerPid;
    private final int datacenterPid;

    public ProtocolMapper(String Prefix) {
        this.datacenterPid = Configuration.getPid("datacenter");
        this.brokerPid = Configuration.getPid("broker");
    }

    @Override
    public void nextCycle(Node node, int protocolID) {
        Type type = nodeType.get(node.getID());
        // Only datacenters should start actions
        if (type == Type.DATACENTER) {
            DatacenterProtocol datacenter = (DatacenterProtocol) node.getProtocol(datacenterPid);
            datacenter.nextCycle(node, protocolID);
        }
    }

    @Override
    public void processEvent(Node node, int pid, Object event) {
        Type type = nodeType.get(node.getID());
        if (type == Type.DATACENTER) {
            DatacenterProtocol datacenter = (DatacenterProtocol) node.getProtocol(datacenterPid);
            datacenter.processEvent(node, pid, event);
        } else if (type == Type.BROKER) {
            BrokerProtocol broker = (BrokerProtocol) node.getProtocol(brokerPid);
            broker.processEvent(node, pid, event);
        } else {
            throw new RuntimeException("Unknown brokerPid type.");
        }
    }

    @Override
    public Object clone() {
        return new ProtocolMapper(null);
    }
}
