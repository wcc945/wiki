# 一. 题目





带过期时间的LRU.

# 二. 思路





维护双向链表，用于最近最久未使用。



单独node维护k、v、l、r、expireTime。



注意：很多题解，直接put完然后检查是否超限，若超则清除尾巴。但是，双链中可能有已经过期的，且尾巴没过期，其实应该先清理过期，再判断是否超限。

# 三. 代码
````java
import java.util.HashMap;
import java.util.Map;

public class LRUCache {

    Map<Integer, Node> cache=new HashMap();
    int cap;
    Node head=new Node(0, 0, 0), tail=new Node(0, 0, 0);

    public LRUCache(int capacity) {
        this.cap=capacity;
        head.r=tail;
        tail.l=head;
    }

    public int get(int key) {
        if(!cache.containsKey(key)) return -1;
        Node cur=cache.get(key);
        if(checkAndRemoveExpireNode(cur)) return -1;//过期
        moveToHead(cur);
        return cur.v;
    }

    public void put(int key, int value, long expireTime) {
        Node cur;
        expireTime += System.currentTimeMillis();
        if(cache.containsKey(key)) {//包含直接更新
            cur=cache.get(key);
            cur.v=value;
            cur.expireTime=expireTime;
            moveToHead(cur);
            return;
        }

        if(cache.size()==cap && !removeExpireNodes()) {//到顶，且没有过期的，则删除尾巴
            Node t=tail.l;
            cache.remove(t.k);
            remove(t);
        }

        cur=new Node(key, value, expireTime);
        cache.put(key, cur);
        addHead(cur);
    }

    boolean removeExpireNodes() {
        Node cur=head.r;
        boolean isRemoved=false;
        while(cur!=tail) {
            Node ne=cur.r;
            isRemoved|=checkAndRemoveExpireNode(cur);
            cur=ne;
        }
        return isRemoved;
    }

    boolean checkAndRemoveExpireNode(Node cur) {
        if(cur.expireTime < System.currentTimeMillis()) {
            cache.remove(cur.k);
            remove(cur);
            return true;
        }
        return false;
    }

    void moveToHead(Node cur) {
        remove(cur);
        addHead(cur);
    }

    void remove(Node cur) {
        cur.l.r=cur.r;
        cur.r.l=cur.l;
    }

    void addHead(Node cur) {
        cur.l=head;
        cur.r=head.r;
        head.r.l=cur;
        head.r=cur;
    }

    static class Node {
        int k, v;
        Node l, r;
        long expireTime;

        public Node(int k, int v, long expireTime) {
            this.k=k;
            this.v=v;
            this.expireTime=expireTime;
        }
    }
}
````