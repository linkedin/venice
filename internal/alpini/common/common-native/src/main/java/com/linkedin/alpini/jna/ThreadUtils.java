package com.linkedin.alpini.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.WString;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public final class ThreadUtils {
  private static final Logger LOG = LogManager.getLogger(ThreadUtils.class);

  private ThreadUtils() {
  }

  public enum OsPlatform {
    LINUX, DARWIN, WINDOWS, UNKNOWN
  }

  @Nonnull
  public static OsPlatform getPlatform() {
    return INTERFACE.getPlatform();
  }

  @Nonnull
  public static ThreadFactory decorateName(@Nonnull ThreadFactory factory) {
    return r -> factory.newThread(() -> {
      setThreadName();
      r.run();
    });
  }

  public static void setThreadName() {
    setThreadName(Thread.currentThread().getName());
  }

  public static void setThreadName(@Nonnull String name) {
    INTERFACE.setThreadName(name);
  }

  private interface NativeInterface {
    @Nonnull
    OsPlatform getPlatform();

    void setThreadName(String name);
  }

  public interface LinuxCLib extends Library {
    int PR_SET_NAME = 15;

    @SuppressWarnings("UnusedReturnValue")
    int prctl(int option, String arg2, long arg3, long arg4, long arg5); // SUPPRESS CHECKSTYLE
                                                                         // RegexpSinglelineJavaCheck
  }

  public interface DarwinCLib extends Library {
    @SuppressWarnings("UnusedReturnValue")
    int pthread_setname_np(String name); // SUPPRESS CHECKSTYLE MethodNameCheck
  }

  public interface WindowsCLib extends Library {
    int GetCurrentThread(); // SUPPRESS CHECKSTYLE MethodNameCheck

    @SuppressWarnings("UnusedReturnValue")
    int SetThreadDescription(int tid, WString name); // SUPPRESS CHECKSTYLE MethodNameCheck
  }

  private static final NativeInterface INTERFACE = initNativeInterface();

  private static NativeInterface initNativeInterface() {
    try {
      switch (Platform.getOSType()) {
        case Platform.LINUX:
          LinuxCLib linuxLib = Native.loadLibrary(LinuxCLib.class);
          return new NativeInterface() {
            @Nonnull
            @Override
            public OsPlatform getPlatform() {
              return OsPlatform.LINUX;
            }

            @Override
            public void setThreadName(String name) {
              linuxLib.prctl(LinuxCLib.PR_SET_NAME, name, 0, 0, 0);
            }
          };

        case Platform.WINDOWS:
          WindowsCLib windowsLib = Native.loadLibrary(WindowsCLib.class);
          return new NativeInterface() {
            @Nonnull
            @Override
            public OsPlatform getPlatform() {
              return OsPlatform.WINDOWS;
            }

            @Override
            public void setThreadName(String name) {
              windowsLib.SetThreadDescription(windowsLib.GetCurrentThread(), new WString(name));
            }
          };

        case Platform.MAC:
          DarwinCLib macLib = Native.loadLibrary(DarwinCLib.class);
          return new NativeInterface() {
            @Nonnull
            @Override
            public OsPlatform getPlatform() {
              return OsPlatform.DARWIN;
            }

            @Override
            public void setThreadName(String name) {
              macLib.pthread_setname_np(name);
            }
          };

        default:
          break;
      }
    } catch (Throwable ex) {
      LOG.debug("Failed to load platform-specific shim", ex);
    }

    LOG.warn("Unable to load a native interface");
    return new NativeInterface() {
      @Nonnull
      @Override
      public OsPlatform getPlatform() {
        return OsPlatform.UNKNOWN;
      }

      @Override
      public void setThreadName(String name) {
        // do nothing
      }
    };
  }
}
