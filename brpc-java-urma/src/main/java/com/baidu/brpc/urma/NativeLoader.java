/*
 * brpc-java JNI 适配 URMA —— Native 库加载工具
 *
 * 职责：加载 libbrpc_java_urma.so
 *
 * 查找顺序（命中即止）：
 *   1. 环境变量 BRPC_JAVA_URMA_LIB 指定的 .so 绝对路径（部署时可显式指定）
 *   2. java.library.path（System.loadLibrary）
 *   3. 运行时目录下的 native/ 子目录、运行时目录本身
 *   4. JAR 所在目录下的 native/ 子目录（便于 JAR 旁置 .so）
 *   5. classpath 内的 native/<libName>（抽取到临时目录后加载）
 *
 * 加载失败时抛出 UnsatisfiedLinkError，并在消息里列出所有尝试过的路径，
 * 便于定位部署问题（不依赖任何机器相关的硬编码目录）。
 */
package com.baidu.brpc.urma;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public final class NativeLoader {

    /** 显式指定 .so 绝对路径的环境变量名。 */
    public static final String LIB_PATH_ENV = "BRPC_JAVA_URMA_LIB";

    private static volatile boolean loaded = false;

    private NativeLoader() {}

    /**
     * 加载 native 库。已加载则直接返回（线程安全：synchronized + volatile 双重检查）。
     *
     * @param libName 不带 lib 前缀和 .so 后缀的名字，如 "brpc_java_urma"
     * @throws UnsatisfiedLinkError 加载失败
     */
    public static synchronized void load(String libName) {
        if (loaded) {
            return;
        }

        String fileName = System.mapLibraryName(libName);  // → libbrpc_java_urma.so
        List<String> tried = new ArrayList<String>();

        // 1) 环境变量显式指定
        String explicit = System.getenv(LIB_PATH_ENV);
        if (explicit != null && !explicit.trim().isEmpty()) {
            File f = new File(explicit.trim());
            tried.add(f.getAbsolutePath());
            if (f.isFile() && f.canRead()) {
                System.load(f.getAbsolutePath());
                loaded = true;
                return;
            }
        }

        // 2) java.library.path
        try {
            System.loadLibrary(libName);
            loaded = true;
            return;
        } catch (UnsatisfiedLinkError ignored) {
            tried.add("java.library.path=" + System.getProperty("java.library.path"));
        }

        // 3) 运行时目录（含 native/ 子目录）、4) JAR 同级 native/ 目录
        String userDir = System.getProperty("user.dir");
        List<File> candidates = new ArrayList<File>();
        if (userDir != null) {
            candidates.add(new File(userDir, "native" + File.separator + fileName));
            candidates.add(new File(userDir, fileName));
        }
        File jarDir = codeSourceDir();
        if (jarDir != null) {
            candidates.add(new File(jarDir, "native" + File.separator + fileName));
            candidates.add(new File(jarDir, fileName));
        }
        for (File f : candidates) {
            tried.add(f.getAbsolutePath());
            if (f.isFile() && f.canRead()) {
                System.load(f.getAbsolutePath());
                loaded = true;
                return;
            }
        }

        // 5) classpath 抽取（JAR 内打包 .so 的场景）
        try {
            Path tmp = extractToTemp(fileName);
            tried.add("classpath:native/" + fileName + " -> " + tmp);
            System.load(tmp.toAbsolutePath().toString());
            loaded = true;
            return;
        } catch (Exception e) {
            tried.add("classpath:native/" + fileName + " not found");
        }

        throw new UnsatisfiedLinkError("Failed to load " + fileName
            + "; tried: " + tried
            + ". 可用 " + LIB_PATH_ENV + " 环境变量或 -Djava.library.path 指定所在目录。");
    }

    /** JAR / classes 目录所在位置，取不到时返回 null。 */
    private static File codeSourceDir() {
        try {
            URL loc = NativeLoader.class.getProtectionDomain().getCodeSource().getLocation();
            if (loc == null || !"file".equals(loc.getProtocol())) {
                return null;
            }
            File f = new File(loc.toURI());
            return f.isDirectory() ? f : f.getParentFile();
        } catch (Exception e) {
            return null;
        }
    }

    private static Path extractToTemp(String fileName) throws IOException {
        ClassLoader cl = NativeLoader.class.getClassLoader();
        try (InputStream in = cl.getResourceAsStream("native/" + fileName)) {
            if (in == null) {
                throw new IOException("classpath:native/" + fileName + " not found");
            }
            Path tmp = Files.createTempFile("brpc-java-urma-", "-" + fileName);
            Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
            tmp.toFile().deleteOnExit();
            return tmp;
        }
    }
}
