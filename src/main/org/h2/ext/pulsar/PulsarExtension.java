package org.h2.ext.pulsar;

import org.h2.engine.ConnectionInfo;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Session;
import org.h2.engine.SessionFactory;
import org.h2.engine.SessionInterface;
import org.h2.engine.UserAggregate;
import org.h2.util.JdbcUtils;
import org.h2.util.Utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Vincent Zhang ivincent.zhang@gmail.com 2020/08/04 ~ 2026/01/14
 * */
public class PulsarExtension {
    public static SessionFactory sessionFactory;

    public PulsarExtension() {
    }

    public static Class<?> getEngineImplementation() {
        String className = Utils.getProperty("h2.sessionFactory", "org.h2.engine.Engine");
        return JdbcUtils.loadUserClass(className);
    }

    public static SessionInterface createSession(ConnectionInfo ci) throws Exception {
        if (sessionFactory == null) {
            Class<?> engine = getEngineImplementation();
            System.err.println("H2 Engine implementation is: " + engine);
            sessionFactory = (SessionFactory)engine.getMethod("getInstance").invoke((Object)null);
        }

        return sessionFactory.createSession(ci);
    }

    public static void closeSession(Session session) throws Exception {
        sessionFactory.closeSession(session.getSerialId());
    }

    public static void shutdownSessionFactory() {
        Optional<Method> shutdownNow = Arrays.stream(sessionFactory.getClass().getMethods()).filter((method) -> {
            return method.getName().equals("shutdownNow");
        }).findFirst();
        if (shutdownNow.isPresent()) {
            try {
                ((Method)shutdownNow.get()).invoke(sessionFactory);
            } catch (InvocationTargetException | IllegalAccessException var2) {
                var2.printStackTrace();
            }
        }
    }

    public static FunctionAlias findFunction(ConcurrentHashMap<String, FunctionAlias> functions, String functionAlias) {
        FunctionAlias f = functions.get(functionAlias);
        if (f != null) {
            return f;
        }

        // We support arbitrary "_" in a UDF name, for example, the following functions are the same:
        // some_fun_(), _____some_fun_(), some______fun()
        functionAlias = functionAlias.replaceAll("_", "");
        return functions.get(functionAlias);
    }

    public static UserAggregate findAggregate(ConcurrentHashMap<String, UserAggregate> aggregates, String name) {
        UserAggregate agg = aggregates.get(name);
        if (agg != null) {
            return agg;
        }

        // We support arbitrary "_" in a UDA name, for example, the following functions are the same:
        // some_fun_(), _____some_fun_(), some______fun()
        name = name.replaceAll("_", "");
        return aggregates.get(name);
    }
}
