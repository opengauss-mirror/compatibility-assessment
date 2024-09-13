/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.agent.impl;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.HashSet;
import java.util.Set;

import org.kit.agent.common.Constant;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * PreparedFormer
 *
 * @author liu
 * @since 2023-09-17
 */
public class PreparedFormer implements ClassFileTransformer {
    private static final Set<String> EXECUTE_LISTEN_SET = new HashSet<>();

    static {
        EXECUTE_LISTEN_SET.add("execute");
        EXECUTE_LISTEN_SET.add("executeUpdate");
        EXECUTE_LISTEN_SET.add("executeQuery");
        EXECUTE_LISTEN_SET.add("addBatch");
    }
    
    @Override
    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain
            protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        if (Constant.PRECLASSNAME.contains(className)) {
            return transformClass(classfileBuffer);
        }
        return classfileBuffer;
    }

    private byte[] transformClass(byte[] classfileBuffer) {
        ClassReader cr = new ClassReader(classfileBuffer);
        ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);
        ClassVisitor cv = new PreparedClassVisitor(cw);
        cr.accept(cv, 0);
        return cw.toByteArray();
    }

    private class PreparedClassVisitor extends ClassVisitor {
        private String className;

        public PreparedClassVisitor(ClassVisitor classVisitor) {
            super(Opcodes.ASM7, classVisitor);
        }

        @Override
        public void visit(int version, int access, String name, String signature,
                          String superName, String[] interfaces) {
            super.visit(version, access, name, signature, superName, interfaces);
            this.className = name;
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor,
                                         String signature, String[] exceptions) {
            MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
            if (EXECUTE_LISTEN_SET.contains(name)) {
                return new PreparedMethodVisitor(mv);
            }
            return mv;
        }

        private class PreparedMethodVisitor extends MethodVisitor {
            public PreparedMethodVisitor(MethodVisitor mv) {
                super(Opcodes.ASM7, mv);
            }

            @Override
            public void visitCode() {
                super.visitCode();
                // 将 this 对象加载到栈顶
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, className, "toString",
                        "()Ljava/lang/String;", false);
                mv.visitVarInsn(Opcodes.ASTORE, 1);
                mv.visitVarInsn(Opcodes.ALOAD, 1);
                // 重新加载变量1的值到栈顶
                mv.visitVarInsn(Opcodes.ALOAD, 1);
                // 调用 sqlRecord 方法
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, Constant.RECORD, "sqlRecord",
                        "(Ljava/lang/String;Ljava/lang/String;)V", false);
                // 将方法参数加载到栈顶
                mv.visitVarInsn(Opcodes.ALOAD, 1);
                // 获取currentThhread
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Thread", "currentThread",
                        "()Ljava/lang/Thread;", false);
                // 获取getStackTrace
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Thread", "getStackTrace",
                        "()[Ljava/lang/StackTraceElement;", false);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, Constant.RECORD, "stakeRecord",
                        "(Ljava/lang/String;[Ljava/lang/StackTraceElement;)V", false);
            }
        }
    }
}

