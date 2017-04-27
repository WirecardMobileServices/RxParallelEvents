package de.wirecard.rxparallel;

import io.reactivex.Single;

public class MainClass {

    public static void main(String[] args) {
        final Integer integer = Single.just("5")
                .map(Integer::valueOf)
                .blockingGet();
        System.out.println(integer);
    }
}
