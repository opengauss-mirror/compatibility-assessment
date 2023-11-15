/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.agent.impl;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import org.kit.agent.common.Constant;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * StateFormer
 *
 * @author liu
 * @since 2023-09-17
 */
public class StateFormer implements ClassFileTransformer {
    @Override
    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain
            protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        if (Constant.STATCLASSNAME.contains(className)) {
            return transformClass(classfileBuffer);
        }
        return classfileBuffer;
    }

    private byte[] transformClass(byte[] classfileBuffer) {
        ClassReader cr = new ClassReader(classfileBuffer);
        ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);
        ClassVisitor cv = new StateClassVisitor(cw);
        cr.accept(cv, 0);
        return cw.toByteArray();
    }

    private class StateClassVisitor extends ClassVisitor {
        private String className;

        public StateClassVisitor(ClassVisitor classVisitor) {
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
            Boolean isExecuteQuery = name.equals("executeQuery") && descriptor
                    .equals("(Ljava/lang/String;)Ljava/sql/ResultSet;");
            Boolean isExecuteUpdate = name.equals("executeUpdate") && descriptor.equals("(Ljava/lang/String;)I");
            Boolean isExecute = name.equals("execute");
            if (isExecute || isExecuteQuery || isExecuteUpdate) {
                return new StateMethodVisitor(mv);
            }
            return mv;
        }

        private class StateMethodVisitor extends MethodVisitor {
            public StateMethodVisitor(MethodVisitor mv) {
                super(Opcodes.ASM7, mv);
            }

            @Override
            public void visitCode() {
                super.visitCode();
                // 将classNmae加载到栈顶 stakeRecord
                mv.visitVarInsn(Opcodes.ALOAD, 1);
                // 获取currentThhread
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Thread", "currentThread",
                        "()Ljava/lang/Thread;", false);
                // 获取getStackTrace
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Thread", "getStackTrace",
                        "()[Ljava/lang/StackTraceElement;", false);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, Constant.RECORD, "stakeRecord",
                        "(Ljava/lang/String;[Ljava/lang/StackTraceElement;)V", false);
                // sqlRecord
                mv.visitLdcInsn(className);
                mv.visitVarInsn(Opcodes.ALOAD, 1);
                // 调用 sqlRecord 方法
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, Constant.RECORD, "sqlRecord",
                        "(Ljava/lang/String;Ljava/lang/String;)V", false);
            }
        }
    }
}

