package org.goraj.sparkcourse.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Data
public final class Movie implements Serializable {
    private int movieID;
}
