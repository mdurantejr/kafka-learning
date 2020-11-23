package com.durante.avro.reflection;

import org.apache.avro.reflect.Nullable;

public class ReflectedCustomer {

    private String firstName;
    private String lastName;
    @Nullable
    private String nickName;

    // Required for the reflection
    public ReflectedCustomer() {
    }

    public ReflectedCustomer(final String firstName, final String lastName, final String nickName) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.nickName = nickName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(final String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(final String lastName) {
        this.lastName = lastName;
    }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(final String nickName) {
        this.nickName = nickName;
    }

    public String fullName() {
        return firstName + " " + lastName + " " + nickName;
    }
}
