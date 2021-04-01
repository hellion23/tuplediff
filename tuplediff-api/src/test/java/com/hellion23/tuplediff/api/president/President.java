package com.hellion23.tuplediff.api.president;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A Java bean version of the presidents csv and json objects. This is used in various tests for tuplediff.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class President {
    int presidentId;
    String firstName;
    String lastName;
    int dob;
    int presidentNo;

    public int getPresidentId() {
        return presidentId;
    }

    public void setPresidentId(int presidentId) {
        this.presidentId = presidentId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public int getDob() {
        return dob;
    }

    public void setDob(int dob) {
        this.dob = dob;
    }

    public int getPresidentNo() {
        return presidentNo;
    }

    public void setPresidentNo(int presidentNo) {
        this.presidentNo = presidentNo;
    }
}
