package ch.ethz.systems.netbench.xpt.aifo.ports.APPIFO;

import ch.ethz.systems.netbench.core.log.SimulationLogger;
import ch.ethz.systems.netbench.core.network.NetworkDevice;
import ch.ethz.systems.netbench.core.network.Packet;
import ch.ethz.systems.netbench.xpt.tcpbase.FullExtTcpPacket;
import ch.ethz.systems.netbench.xpt.tcpbase.PriorityHeader;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;


public class APPIFOQueue implements Queue {

    private final ArrayList<ArrayBlockingQueue<Packet>> queueList;
    private final BitSet fullQueues;
    private final int[] packetCounts;
    private final int numQueues;
    private final int perQueueCapacity;
    private int highestPriorityQueueIndex;
    private int ownId;
    private ReentrantLock reentrantLock;

    public APPIFOQueue(long numQueues, long perQueueCapacity, NetworkDevice ownNetworkDevice){
        this.numQueues = (int) numQueues;
        this.perQueueCapacity = (int) perQueueCapacity;
        this.queueList = new ArrayList<>(this.numQueues);
        this.fullQueues = new BitSet(this.numQueues);
        this.packetCounts = new int[this.numQueues];
        this.highestPriorityQueueIndex = 0;
        this.ownId = ownNetworkDevice.getIdentifier();
        this.reentrantLock = new ReentrantLock();

        for (int i = 0; i < this.numQueues; i++) {
            queueList.add(new ArrayBlockingQueue<>(this.perQueueCapacity));
        }
    }

    // Packet dropped and null returned if selected queue exceeds its size
    @Override
    public boolean offer(Object o) {

        reentrantLock.lock();
        try {
            // Extract rank from header
            Packet packet = (Packet) o;
            PriorityHeader header = (PriorityHeader) packet;
            int pre_rank = (int) header.getPriority();
            // System.out.println("rank of the incoming packet is "+ pre_rank);

            double r = (double) pre_rank / 1000000 * 7; // Scale to 0-7 (total 8 queues)
            int rank = (int) r; // Cast to integer

            // System.out.println("mapped rank of the incoming packet is "+ rank);
            // System.out.println("highest priority queue is "+ highestPriorityQueueIndex);


            int targetQueue = (highestPriorityQueueIndex + rank) % numQueues;

            // System.out.println("target queue id is "+ targetQueue);

            if (!fullQueues.get(targetQueue)) {
                boolean added = queueList.get(targetQueue).offer(packet);
                if (added) {
                    // System.out.println("packet with the rank "+rank+ " is added to the queue "+ targetQueue);
                    packetCounts[targetQueue]++;
                    if (packetCounts[targetQueue] == perQueueCapacity) {
                        fullQueues.set(targetQueue);
                    }
                    return true;
                }
            }
            else{
                // System.out.println("target queue is full!\n serching in lower priority queues.........");
                for (int i = (targetQueue -1 + numQueues) % numQueues; i != highestPriorityQueueIndex; i = (i - 1 + numQueues) % numQueues)
                {
                    if (!fullQueues.get(i)) {
                        boolean added = queueList.get(i).offer(packet);
                        if (added) {
                            // System.out.println("packet with the rank "+rank+ " is added to the queue "+ i);
                            packetCounts[i]++;
                            if (packetCounts[i] == perQueueCapacity) {
                                fullQueues.set(i);
                            }
                            return true;
                        }
                    }
                    // System.out.println("queue "+i+" is full..");
                }
                // System.out.println("all the queues less then target queue are empty!");
            }
            // System.out.println("all the queues in the switch are full");
            return false; // All queues are full
        } finally {
            reentrantLock.unlock();
            // try {
            //     TimeUnit.SECONDS.sleep(1); // Wait for 5 seconds
            // } catch (InterruptedException e) {
            //     System.err.println("Thread interrupted: " + e.getMessage());
            // }
        }
    }

    @Override
    public Packet poll() {
        reentrantLock.lock();
        try {
            if (isEmpty()) {
                return null;
            }

            // rotate the higher priority queue if the all the packets from the queue are dequeued or queue is empty
            // it should be done on time basis

            // System.out.println("dequeuing the packet...........");
            while (packetCounts[highestPriorityQueueIndex] == 0) {
                // System.out.println("changing the queue prioriy");
                rotateQueuePriorities();
                // System.out.println("head is "+highestPriorityQueueIndex);
            }

            Packet p = queueList.get(highestPriorityQueueIndex).poll();

            if (p != null) {
                // System.out.println("packet is dequeued form the highest priority queue "+ highestPriorityQueueIndex);
                packetCounts[highestPriorityQueueIndex]--;
                fullQueues.clear(highestPriorityQueueIndex);

                // PriorityHeader header = (PriorityHeader) p;
                // int rank = (int) header.getPriority();
                return p;
            }
            // if (packetCounts[highestPriorityQueueIndex] == 0) {
            //     rotateQueuePriorities();
            // }

            return null;
        } finally {
            reentrantLock.unlock();
            // try {
            //     TimeUnit.SECONDS.sleep(1); // Wait for 5 seconds
            // } catch (InterruptedException e) {
            //     System.err.println("Thread interrupted: " + e.getMessage());
            // }
        }
    }

    private void rotateQueuePriorities() {
        highestPriorityQueueIndex = (highestPriorityQueueIndex + 1) % numQueues;
    }

    @Override
    public int size() {
        int size = 0;
        for (int q=0; q<queueList.size(); q++){
            size += queueList.get(q).size();
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        boolean empty = true;
        for (int q=0; q<queueList.size(); q++){
            if(!queueList.get(q).isEmpty()){
                empty = false;
            }
        }
        return empty;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public Object[] toArray(Object[] objects) {
        return new Object[0];
    }

    @Override
    public boolean add(Object o) {
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean addAll(Collection collection) {
        return false;
    }

    @Override
    public void clear() { }

    @Override
    public boolean retainAll(Collection collection) {
        return false;
    }

    @Override
    public boolean removeAll(Collection collection) {
        return false;
    }

    @Override
    public boolean containsAll(Collection collection) {
        return false;
    }

    @Override
    public Object remove() {
        return null;
    }

    @Override
    public Object element() {
        return null;
    }

    @Override
    public Object peek() {
        return null;
    }
}
