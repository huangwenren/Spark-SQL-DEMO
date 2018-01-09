package edu.tongji.entity;


public class Call {
    private Integer day_id;
    private String calling_nbr;
    private String called_nbr;
    private Integer calling_optr;
    private Integer called_optr;
    private String calling_city;
    private String called_city;
    private String calling_roam_city;
    private String called_roam_city;
    private String start_time;
    private String end_time;
    private Integer raw_dur;
    private Integer call_type;
    private String calling_cell;

    public Integer getDay_id() {
        return day_id;
    }

    public void setDay_id(Integer day_id) {
        this.day_id = day_id;
    }

    public String getCalling_nbr() {
        return calling_nbr;
    }

    public void setCalling_nbr(String calling_nbr) {
        this.calling_nbr = calling_nbr;
    }

    public String getCalled_nbr() {
        return called_nbr;
    }

    public void setCalled_nbr(String called_nbr) {
        this.called_nbr = called_nbr;
    }

    public Integer getCalling_optr() {
        return calling_optr;
    }

    public void setCalling_optr(Integer calling_optr) {
        this.calling_optr = calling_optr;
    }

    public Integer getCalled_optr() {
        return called_optr;
    }

    public void setCalled_optr(Integer called_optr) {
        this.called_optr = called_optr;
    }

    public String getCalling_city() {
        return calling_city;
    }

    public void setCalling_city(String calling_city) {
        this.calling_city = calling_city;
    }

    public String getCalled_city() {
        return called_city;
    }

    public void setCalled_city(String called_city) {
        this.called_city = called_city;
    }

    public String getCalling_roam_city() {
        return calling_roam_city;
    }

    public void setCalling_roam_city(String calling_roam_city) {
        this.calling_roam_city = calling_roam_city;
    }

    public String getCalled_roam_city() {
        return called_roam_city;
    }

    public void setCalled_roam_city(String called_roam_city) {
        this.called_roam_city = called_roam_city;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public Integer getRaw_dur() {
        return raw_dur;
    }

    public void setRaw_dur(Integer raw_dur) {
        this.raw_dur = raw_dur;
    }

    public Integer getCall_type() {
        return call_type;
    }

    public void setCall_type(Integer call_type) {
        this.call_type = call_type;
    }

    public String getCalling_cell() {
        return calling_cell;
    }

    public void setCalling_cell(String calling_cell) {
        this.calling_cell = calling_cell;
    }

    @Override
    public String toString() {
        return "Call{" +
                "day_id=" + day_id +
                ", calling_nbr='" + calling_nbr + '\'' +
                ", called_nbr='" + called_nbr + '\'' +
                ", calling_optr=" + calling_optr +
                ", called_optr=" + called_optr +
                ", calling_city='" + calling_city + '\'' +
                ", called_city='" + called_city + '\'' +
                ", calling_roam_city='" + calling_roam_city + '\'' +
                ", called_roam_city='" + called_roam_city + '\'' +
                ", start_time='" + start_time + '\'' +
                ", end_time='" + end_time + '\'' +
                ", raw_dur=" + raw_dur +
                ", call_type=" + call_type +
                ", calling_cell='" + calling_cell + '\'' +
                '}';
    }

}

