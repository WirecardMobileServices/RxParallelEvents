package de.wirecard.rxparallel.util;


import com.jakewharton.rxrelay2.Relay;

import io.reactivex.Observer;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;

public class RelayToObserver<T> implements Observer<T> {

    private Relay<T> relay;

    public RelayToObserver(Relay<T> relay) {
        this.relay = relay;
    }

    @Override
    public void onSubscribe(@NonNull Disposable d) {

    }

    @Override
    public void onNext(@NonNull T t) {
        if (relay != null)
            relay.accept(t);
    }

    @Override
    public void onError(@NonNull Throwable e) {

    }

    @Override
    public void onComplete() {

    }
}
