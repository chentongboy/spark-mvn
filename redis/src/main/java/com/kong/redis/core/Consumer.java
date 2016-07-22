package com.kong.redis.core;

import com.kong.redis.base.BaseServiceImpl;
import com.kong.redis.exception.CacheException;

import java.util.ArrayList;
import java.util.List;

/**
 * 负责消息队列任务
 * Created by kong on 2016/4/30.
 */
public class Consumer extends BaseServiceImpl<AyscQueue> {
    Thread queueTaskThread = null;

    private List<AyscQueue> ayscQueueList = new ArrayList<AyscQueue>();

    private boolean threadNeedStop = false;

    int checkpoint = 1;

    int ct = 0;
    int sleepTime = 5;
    int checkStateTime = 10;

    boolean isPrintState() {
        return ct == (checkStateTime / sleepTime);
    }

    /**
     * 取得cache对象执行业务逻辑（由popAyscQueue()实际指定）
     * @return
     * @throws Exception
     */
    public boolean run() throws Exception {
        queueTaskThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Object obj = null;
                    try {
                        obj = pop(AyscQueue.AyscQueueKey, false);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (obj != null) {
                        AyscQueue ayscQueue = (AyscQueue) obj;
                        try {
                            ayscQueue.popAyscQueue();
                        } catch (CacheException e) {
                            e.printStackTrace();
                        }
                    } else {
                        if (threadNeedStop)
                            break;
                        try {
                            Thread.sleep(sleepTime, 1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        queueTaskThread.start();
        return true;
    }

}
