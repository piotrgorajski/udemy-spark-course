package org.goraj.sparkcourse.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public final class Person implements Serializable {
    private int ID;
    private String name;
    private int age;
    private int numFriends;
}
