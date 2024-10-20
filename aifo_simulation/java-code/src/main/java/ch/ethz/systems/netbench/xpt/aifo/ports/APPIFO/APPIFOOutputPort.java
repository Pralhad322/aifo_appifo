package ch.ethz.systems.netbench.xpt.aifo.ports.APPIFO;

import ch.ethz.systems.netbench.core.Simulator;
import ch.ethz.systems.netbench.core.log.SimulationLogger;
import ch.ethz.systems.netbench.core.network.*;
import ch.ethz.systems.netbench.ext.basic.IpHeader;
import ch.ethz.systems.netbench.xpt.tcpbase.FullExtTcpPacket;


public class APPIFOOutputPort extends OutputPort {

    public APPIFOOutputPort(NetworkDevice ownNetworkDevice, NetworkDevice targetNetworkDevice, Link link, long numberQueues, long sizePerQueuePackets, String stepSize) {
        super(ownNetworkDevice, targetNetworkDevice, link, new APPIFOQueue1(numberQueues, sizePerQueuePackets, ownNetworkDevice, stepSize));
    }

    /**
     * Enqueue the given packet.
     * To control droppings
     * There is no guarantee that the packet is actually sent,
     * as the queue buffer's limit might be reached.
     * @param packet    Packet instance
     */
    @Override
    public void enqueue(Packet packet) {

        // Enqueue packet
        potentialEnqueue(packet);
    }


}
