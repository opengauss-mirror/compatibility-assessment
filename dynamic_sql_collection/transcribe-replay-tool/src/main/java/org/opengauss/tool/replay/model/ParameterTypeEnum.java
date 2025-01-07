/*
 * Copyright (c) 2024-2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.tool.replay.model;

import org.postgresql.util.PGobject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalTime;

/**
 * ParameterTypeEnum
 *
 * @since 2024-07-01
 */
public enum ParameterTypeEnum {
    /**
     * int
     */
    INT {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            preSqlStmt.setInt(paramIndex, Integer.parseInt(paramValue));
        }
    },
    /**
     * double
     */
    DOUBLE {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            preSqlStmt.setDouble(paramIndex, Double.parseDouble(paramValue));
        }
    },
    /**
     * string
     */
    STRING {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            preSqlStmt.setString(paramIndex, paramValue);
        }
    },
    /**
     * timestamp
     */
    TIMESTAMP {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            try {
                preSqlStmt.setTimestamp(paramIndex, Timestamp.valueOf(paramValue));
            } catch (IllegalArgumentException e) {
                throw new SQLException("Invalid timestamp format: " + paramValue, e);
            }
        }
    },
    NUMERIC {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            preSqlStmt.setObject(paramIndex, paramValue);
        }
    },
    /**
     * null
     */
    NULL {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            preSqlStmt.setNull(paramIndex, Types.VARCHAR);
        }
    },
    /**
     * object
     */
    OBJECT {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            preSqlStmt.setObject(paramIndex, paramValue);
        }
    },
    /**
     * date
     */
    DATE {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            preSqlStmt.setDate(paramIndex, Date.valueOf(paramValue));
        }
    },
    /**
     * time
     */
    TIME {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            LocalTime localTime = LocalTime.parse(paramValue);
            preSqlStmt.setTime(paramIndex, Time.valueOf(localTime));
        }
    },
    /**
     * long
     */
    LONG {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            preSqlStmt.setLong(paramIndex, Long.parseLong(paramValue));
        }
    },
    /**
     * bytea
     */
    BYTEA {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            byte[] bytes = paramValue.getBytes(StandardCharsets.UTF_8);
            preSqlStmt.setBytes(paramIndex, bytes);
        }
    },
    /**
     * bit
     */
    BIT {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            preSqlStmt.setObject(paramIndex, paramValue, Types.BIT);
        }
    },
    /**
     * blob
     */
    BLOB {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            InputStream blobStream = new ByteArrayInputStream(paramValue.getBytes(StandardCharsets.UTF_8));
            preSqlStmt.setBlob(paramIndex, blobStream);
        }
    },
    /**
     * json
     */
    JSON {
        @Override
        public void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException {
            PGobject jsonObject = new PGobject();
            jsonObject.setType("json");
            jsonObject.setValue(paramValue.replaceAll("\\\\", ""));
            preSqlStmt.setObject(paramIndex, jsonObject);
        }
    };

    /**
     * set PreparedStatement param
     *
     * @param preSqlStmt preSqlStmt
     * @param paramIndex paramIndex
     * @param paramValue paramValue
     * @throws SQLException SQLException
     */
    public abstract void setParam(PreparedStatement preSqlStmt, int paramIndex, String paramValue) throws SQLException;

    /**
     * get enum instance from type name
     *
     * @param typeStr typeStr
     * @return ParameterType
     */
    public static ParameterTypeEnum fromTypeName(String typeStr) {
        for (ParameterTypeEnum type : values()) {
            if (type.name().equalsIgnoreCase(typeStr)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported parameter type: " + typeStr);
    }
}
