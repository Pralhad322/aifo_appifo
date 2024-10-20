package ch.ethz.systems.netbench.xpt.aifo.ports.APPIFO;

import ch.ethz.systems.netbench.core.log.SimulationLogger;
import ch.ethz.systems.netbench.core.network.NetworkDevice;
import ch.ethz.systems.netbench.core.network.Packet;
import ch.ethz.systems.netbench.xpt.tcpbase.FullExtTcpPacket;
import ch.ethz.systems.netbench.xpt.tcpbase.PriorityHeader;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class APPIFOQueue1 implements Queue<Packet> {

    private class TreeNode {
        float lower;
        float upper;
        TreeNode left;
        TreeNode right;
        ArrayBlockingQueue<Packet> queue;

        TreeNode(float bound) {
            this.lower = bound;
            this.upper = bound;
            this.left = null;
            this.right = null;
            this.queue = null;
        }
    }

    private TreeNode root;
    private final long perQueueCapacity;
    private int ownId;
    private int totalQueues;

    public APPIFOQueue1(long numQueues, long perQueueCapacity, NetworkDevice ownNetworkDevice, String stepSize) {
        this.perQueueCapacity = perQueueCapacity;
        this.ownId = ownNetworkDevice.getIdentifier();
        this.totalQueues = (int)numQueues;
        this.root = buildTree(0, totalQueues - 1);
    }

    private TreeNode buildTree(int start, int end) {
        if (start > end) return null;
        
        TreeNode node = new TreeNode(0);
        
        if (start == end) {
            node.queue = new ArrayBlockingQueue<>((int)perQueueCapacity);
            return node;
        }
        
        int mid = (start + end) / 2;
        node.left = buildTree(start, mid);
        node.right = buildTree(mid + 1, end);
        
        node.lower = (node.left.lower + node.left.upper) / 2;
        node.upper = (node.right.lower + node.right.upper) / 2;
        
        return node;
    }

    @Override
    public boolean offer(Packet packet) {
        PriorityHeader header = (PriorityHeader) packet;
        float rank = header.getPriority();

        return offerToNode(root, packet, rank, 0, totalQueues - 1);
    }

    private boolean offerToNode(TreeNode node, Packet packet, float rank, int start, int end) {
        if (node == null) return false;

        if (start == end) {
            if (node.queue.size() == perQueueCapacity) {
                return false;
            }

            boolean result = node.queue.offer(packet);
            if (result) {
                node.lower = node.upper = rank;
                updateBounds(root, start, 0, totalQueues - 1);
            }
            return result;
        }

        int mid = (start + end) / 2;
        float avg = (node.lower + node.upper) / 2;
        if (rank <= avg) {
            boolean result = offerToNode(node.left, packet, rank, start, mid);
            if (result) {
                node.lower = (node.left.lower + node.left.upper) / 2;
            }
            return result;
        } else {
            boolean result = offerToNode(node.right, packet, rank, mid + 1, end);
            if (result) {
                node.upper = (node.right.lower + node.right.upper) / 2;
            }
            return result;
        }
    }

    private void updateBounds(TreeNode node, int updatedQueue, int start, int end) {
        if (node == null) return;

        if (start == end) return;

        int mid = (start + end) / 2;
        if (updatedQueue <= mid) {
            updateBounds(node.left, updatedQueue, start, mid);
            node.lower = (node.left.lower + node.left.upper) / 2;
        } else {
            updateBounds(node.right, updatedQueue, mid + 1, end);
            node.upper = (node.right.lower + node.right.upper) / 2;
        }
    }

    @Override
    public Packet poll() {
        return pollFromNode(root, 0, totalQueues - 1);
    }

    private Packet pollFromNode(TreeNode node, int start, int end) {
        if (node == null) return null;

        if (start == end) {
            Packet p = node.queue.poll();
            if (p != null) {
                PriorityHeader header = (PriorityHeader) p;
                float rank = header.getPriority();

                if(SimulationLogger.hasRankMappingEnabled()){
                    SimulationLogger.logRankMapping(this.ownId, (int)rank, start);
                }

                if(SimulationLogger.hasQueueBoundTrackingEnabled()){
                    logAllQueueBounds(root, 0, totalQueues - 1);
                }

                if (SimulationLogger.hasInversionsTrackingEnabled()) {
                    int count_inversions = countInversions(root, rank);
                    if (count_inversions != 0) {
                        SimulationLogger.logInversionsPerRank(this.ownId, (int)rank, count_inversions);
                    }
                }
            }
            return p;
        }

        int mid = (start + end) / 2;
        Packet leftPacket = pollFromNode(node.left, start, mid);
        if (leftPacket != null) {
            node.lower = (node.left.lower + node.left.upper) / 2;
            return leftPacket;
        }

        Packet rightPacket = pollFromNode(node.right, mid + 1, end);
        if (rightPacket != null) {
            node.upper = (node.right.lower + node.right.upper) / 2;
        }
        return rightPacket;
    }

    private void logAllQueueBounds(TreeNode node, int start, int end) {
        if (node == null) return;
        if (start == end) {
            SimulationLogger.logQueueBound(this.ownId, start, (int)node.lower);
            return;
        }
        int mid = (start + end) / 2;
        logAllQueueBounds(node.left, start, mid);
        logAllQueueBounds(node.right, mid + 1, end);
    }

    private int countInversions(TreeNode node, float rank) {
        if (node == null) return 0;
        if (node.queue != null) {
            int count = 0;
            for (Packet p : node.queue) {
                if (((PriorityHeader)p).getPriority() < rank) {
                    count++;
                }
            }
            return count;
        }
        return countInversions(node.left, rank) + countInversions(node.right, rank);
    }

    @Override
    public int size() {
        return getSizeOfSubtree(root);
    }

    private int getSizeOfSubtree(TreeNode node) {
        if (node == null) return 0;
        if (node.queue != null) return node.queue.size();
        return getSizeOfSubtree(node.left) + getSizeOfSubtree(node.right);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return containsInSubtree(root, (Packet)o);
    }

    private boolean containsInSubtree(TreeNode node, Packet packet) {
        if (node == null) return false;
        if (node.queue != null) return node.queue.contains(packet);
        return containsInSubtree(node.left, packet) || containsInSubtree(node.right, packet);
    }

    @Override
    public Iterator<Packet> iterator() {
        return new Iterator<Packet>() {
            private Queue<Packet> flattenedQueue = flattenTree();
            
            @Override
            public boolean hasNext() {
                return !flattenedQueue.isEmpty();
            }
            
            @Override
            public Packet next() {
                return flattenedQueue.poll();
            }
        };
    }

    private Queue<Packet> flattenTree() {
        Queue<Packet> result = new LinkedList<>();
        flattenTreeHelper(root, result);
        return result;
    }

    private void flattenTreeHelper(TreeNode node, Queue<Packet> result) {
        if (node == null) return;
        if (node.queue != null) {
            result.addAll(node.queue);
            return;
        }
        flattenTreeHelper(node.left, result);
        flattenTreeHelper(node.right, result);
    }

    @Override
    public Object[] toArray() {
        return flattenTree().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return flattenTree().toArray(a);
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Remove operation is not supported for APPIFOQueue1");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends Packet> c) {
        boolean modified = false;
        for (Packet packet : c) {
            if (offer(packet)) {
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("RemoveAll operation is not supported for APPIFOQueue1");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("RetainAll operation is not supported for APPIFOQueue1");
    }

    @Override
    public void clear() {
        clearSubtree(root);
    }

    private void clearSubtree(TreeNode node) {
        if (node == null) return;
        if (node.queue != null) {
            node.queue.clear();
            return;
        }
        clearSubtree(node.left);
        clearSubtree(node.right);
    }

    @Override
    public Packet remove() {
        Packet packet = poll();
        if (packet == null) {
            throw new NoSuchElementException("Queue is empty");
        }
        return packet;
    }

    @Override
    public Packet element() {
        Packet packet = peek();
        if (packet == null) {
            throw new NoSuchElementException("Queue is empty");
        }
        return packet;
    }

    @Override
    public Packet peek() {
        return peekFromSubtree(root);
    }

    private Packet peekFromSubtree(TreeNode node) {
        if (node == null) return null;
        if (node.queue != null) return node.queue.peek();
        Packet leftPeek = peekFromSubtree(node.left);
        if (leftPeek != null) return leftPeek;
        return peekFromSubtree(node.right);
    }

    @Override
    public boolean add(Packet packet) {
        if (offer(packet)) {
            return true;
        } else {
            throw new IllegalStateException("Queue is full");
        }
    }
}
