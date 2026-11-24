/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * transcribe-replay-tool is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.kit.verb;

import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Attach
 *
 * @author liu
 * @since 2023-09-17
 */
@Slf4j
public class Attach {
    public static void main(String[] args) {
        String pid = args[0];
        String path = args[1];
        String action = args[2];
        String agentArgs = args[3];

        if (args.length > 4) {
            agentArgs = String.join(" ", Arrays.copyOfRange(args, 3, args.length));
        }
        try {
            if (action.equals("install")) {
                VirtualMachineDescriptor vmd = findVirtualMachine(pid).get();
                if (vmd != null) {
                    VirtualMachine vm = VirtualMachine.attach(vmd);
                    // 加载 agent
                    vm.loadAgent(path, agentArgs);
                    vm.detach();
                    log.info("Agent successfully loaded to the target process");
                } else {
                    log.error("Could not find a process with PID");
                }
            }
        } catch (IOException | AgentLoadException | AgentInitializationException | AttachNotSupportedException
                exception) {
            log.error("Failed to load/detach agent to/from the target process: " + exception.getMessage());
        }
    }

    private static Optional<VirtualMachineDescriptor> findVirtualMachine(String pid) {
        List<VirtualMachineDescriptor> vmds = VirtualMachine.list();
        for (VirtualMachineDescriptor vmd : vmds) {
            if (vmd.id().equals(pid)) {
                return Optional.of(vmd);
            }
        }
        return Optional.empty();
    }
}