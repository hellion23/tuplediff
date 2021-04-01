package com.hellion23.tuplediff.api.format;

import com.hellion23.tuplediff.api.model.TDException;
import org.springframework.core.GenericTypeResolver;

/**
 * Decorator that can resolve Generic Type to a concrete class. This does not work for Lambdas and will
 * throw an exception if getType is called.
 *
 * Created by hleung on 7/30/2017.
 */
public interface Typed <R> {
    default Class getType() {
        Class type = GenericTypeResolver.resolveTypeArgument(this.getClass(), Typed.class);
        if (type == null && this.getClass().getName().contains("lambda")) {
            throw new TDException("Cannot resolve types for lambda classes. Use an anonymous class, concrete class or override this method.  " + getClass() + " Object: " + this);
        }
        else if (type == null) {
            System.out.println("Cannot resolve types for lambda classes. Use an anonymous class, concrete class or override this method.  " + getClass() + " Object: " + this);
        }
        return type;
    }
}
