package com.kong.hadoop.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 优化版
 * Created by kong on 2016/5/13.
 */
public class WorkerInfo2 implements WritableComparable<WorkerInfo2>{
    private String workerNo = "";
    private String workerName = "";
    private String departmentNo = "";
    private String departmentName = "";

    public WorkerInfo2() {
    }

    public WorkerInfo2(String workerNo, String workerName, String departmentNo, String departmentName, int flag) {
        this.workerNo = workerNo;
        this.workerName = workerName;
        this.departmentNo = departmentNo;
        this.departmentName = departmentName;
    }

    public WorkerInfo2(WorkerInfo2 worker) {
        this.workerNo = worker.workerNo;
        this.workerName = worker.workerName;
        this.departmentNo = worker.departmentNo;
        this.departmentName = worker.departmentName;
    }

    public int compareTo(WorkerInfo2 o) {
        return 0;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.workerNo);
        out.writeUTF(this.workerName);
        out.writeUTF(this.departmentNo);
        out.writeUTF(this.departmentName);
    }

    public void readFields(DataInput in) throws IOException {
        this.workerNo = in.readUTF();
        this.workerName = in.readUTF();
        this.departmentNo = in.readUTF();
        this.departmentName = in.readUTF();
    }

    public String getWorkerNo() {
        return workerNo;
    }

    public void setWorkerNo(String workerNo) {
        this.workerNo = workerNo;
    }

    public String getWorkerName() {
        return workerName;
    }

    public void setWorkerName(String workerName) {
        this.workerName = workerName;
    }

    public String getDepartmentNo() {
        return departmentNo;
    }

    public void setDepartmentNo(String departmentNo) {
        this.departmentNo = departmentNo;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public void setDepartmentName(String departmentName) {
        this.departmentName = departmentName;
    }

    @Override
    public String toString() {
        return "WorkerInfo2{" +
                "workerNo='" + workerNo + '\'' +
                ", workerName='" + workerName + '\'' +
                ", departmentNo='" + departmentNo + '\'' +
                ", departmentName='" + departmentName + '\'' +
                '}';
    }
}
