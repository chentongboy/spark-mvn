package com.kong.hadoop.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 员工与部门连接信息
 * Created by kong on 2016/4/27.
 */
public class WorkerInfo implements WritableComparable {

    private String workerNo = "";
    private String workerName = "";
    private String departmentNo = "";
    private String departmentName = "";
    private int flag = 0; //0代表department,1代表worker

    public WorkerInfo() {
    }

    public WorkerInfo(String workerNo, String workerName, String departmentNo, String departmentName, int flag) {
        this.workerNo = workerNo;
        this.workerName = workerName;
        this.departmentNo = departmentNo;
        this.departmentName = departmentName;
        this.flag = flag;
    }

    public WorkerInfo(WorkerInfo worker) {
        this.workerNo = worker.workerNo;
        this.workerName = worker.workerName;
        this.departmentNo = worker.departmentNo;
        this.departmentName = worker.departmentName;
        this.flag = worker.flag;
    }

    public int compareTo(Object o) {
        return 0;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.workerNo);
        out.writeUTF(this.workerName);
        out.writeUTF(this.departmentNo);
        out.writeUTF(this.departmentName);
        out.writeInt(this.flag);
    }

    public void readFields(DataInput in) throws IOException {
        this.workerNo = in.readUTF();
        this.workerName = in.readUTF();
        this.departmentNo = in.readUTF();
        this.departmentName = in.readUTF();
        this.flag = in.readInt();
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

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "WorkerInfo{" +
                "workerNo='" + workerNo + '\'' +
                ", workerName='" + workerName + '\'' +
                ", departmentNo='" + departmentNo + '\'' +
                ", departmentName='" + departmentName + '\'' +
                ", flag=" + flag +
                '}';
    }
}
