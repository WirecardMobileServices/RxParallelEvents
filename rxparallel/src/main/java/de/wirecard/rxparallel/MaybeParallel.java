package de.wirecard.rxparallel;

import io.reactivex.Maybe;
import io.reactivex.MaybeObserver;
import io.reactivex.Observer;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;
import io.reactivex.subjects.Subject;

public class MaybeParallel<MAYBE, PARALLEL> extends Maybe<MAYBE> {

    private Maybe<MAYBE> mainMaybe;
    private Subject<PARALLEL> parallelSubject;

    private MaybeParallel(Maybe<MAYBE> mainMaybe, Subject<PARALLEL> parallelSubject) {
        if(mainMaybe == null)
            throw new NullPointerException("Main maybe can not be null");
        this.mainMaybe = mainMaybe;
        this.parallelSubject = parallelSubject;
    }

    @Override
    protected void subscribeActual(MaybeObserver<? super MAYBE> observer) {
        mainMaybe.subscribe(observer);
    }

    public Maybe<MAYBE> subscribeParallel(Observer<PARALLEL> parallelObserver) {
        if (this.parallelSubject != null && parallelObserver != null) {
            this.parallelSubject.subscribeWith(parallelObserver);
        }
        return mainMaybe;
    }

    public static <MAYBE, PARALLEL> Function<? super Maybe<MAYBE>, MaybeParallel<MAYBE, PARALLEL>> with(final Subject<PARALLEL> parallelSubject) {
        return new Function<Maybe<MAYBE>, MaybeParallel<MAYBE, PARALLEL>>() {
            @Override
            public MaybeParallel<MAYBE, PARALLEL> apply(@NonNull Maybe<MAYBE> maybe) throws Exception {
                return new MaybeParallel<MAYBE, PARALLEL>(maybe, parallelSubject);
            }
        };
    }
}
