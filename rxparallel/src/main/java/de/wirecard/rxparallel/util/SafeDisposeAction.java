package de.wirecard.rxparallel.util;

import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;

public class SafeDisposeAction implements Action {
    private Disposable disposable;

    private SafeDisposeAction(Disposable disposable) {
        this.disposable = disposable;
    }

    @Override
    public void run() {
        if (disposable != null && !disposable.isDisposed()) {
            disposable.dispose();
        }
    }

    public static SafeDisposeAction createAction(Disposable disposable) {
        return new SafeDisposeAction(disposable);
    }
}
