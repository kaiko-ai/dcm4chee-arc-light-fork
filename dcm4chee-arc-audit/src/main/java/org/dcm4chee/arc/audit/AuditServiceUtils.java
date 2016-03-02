/*
 * *** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is part of dcm4che, an implementation of DICOM(TM) in
 * Java(TM), hosted at https://github.com/gunterze/dcm4che.
 *
 * The Initial Developer of the Original Code is
 * J4Care.
 * Portions created by the Initial Developer are Copyright (C) 2013
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * See @authors listed below
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * *** END LICENSE BLOCK *****
 */
package org.dcm4chee.arc.audit;

import org.dcm4che3.audit.AuditMessages;
import org.dcm4che3.audit.EventTypeCode;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.dcm4che3.util.StringUtils;
import org.dcm4chee.arc.delete.StudyDeleteContext;
import org.dcm4chee.arc.query.QueryContext;
import org.dcm4chee.arc.retrieve.RetrieveContext;
import org.dcm4chee.arc.store.StoreContext;
import org.dcm4chee.arc.store.StoreSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author Vrinda Nayak <vrinda.nayak@j4care.com>
 */
public class AuditServiceUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuditService.class);
    private static final String noValue = "<none>";
    enum EventType {
        WADO_R_P__(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Read, AuditMessages.EventOutcomeIndicator.Success,
                AuditMessages.RoleIDCode.Destination, AuditMessages.RoleIDCode.Source, true, false, false, null),
        WADO_R_E__(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Read, AuditMessages.EventOutcomeIndicator.MinorFailure,
                AuditMessages.RoleIDCode.Destination, AuditMessages.RoleIDCode.Source, true, false, false, null),
        STORE_C_P_(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Create, AuditMessages.EventOutcomeIndicator.Success,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, true, false, false, null),
        STORE_C_E_(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Create, AuditMessages.EventOutcomeIndicator.MinorFailure,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, true, false, false, null),
        STORE_U_P_(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Update, AuditMessages.EventOutcomeIndicator.Success,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, true, false, false, null),
        STORE_U_E_(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Update, AuditMessages.EventOutcomeIndicator.MinorFailure,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, true, false, false, null),

        BEGIN__M_P(AuditMessages.EventID.BeginTransferringDICOMInstances, AuditMessages.EventActionCode.Execute, AuditMessages.EventOutcomeIndicator.Success,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, false, false, true, null),
        BEGIN__M_E(AuditMessages.EventID.BeginTransferringDICOMInstances, AuditMessages.EventActionCode.Execute, AuditMessages.EventOutcomeIndicator.MinorFailure,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, false, false, true, null),
        BEGIN__G_P(AuditMessages.EventID.BeginTransferringDICOMInstances, AuditMessages.EventActionCode.Execute, AuditMessages.EventOutcomeIndicator.Success,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, false, true, false, null),
        BEGIN__G_E(AuditMessages.EventID.BeginTransferringDICOMInstances, AuditMessages.EventActionCode.Execute, AuditMessages.EventOutcomeIndicator.MinorFailure,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, false, true, false, null),
        BEGIN__E_P(AuditMessages.EventID.BeginTransferringDICOMInstances, AuditMessages.EventActionCode.Execute, AuditMessages.EventOutcomeIndicator.Success,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, true, false, false, null),
        BEGIN__E_E(AuditMessages.EventID.BeginTransferringDICOMInstances, AuditMessages.EventActionCode.Execute, AuditMessages.EventOutcomeIndicator.MinorFailure,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, true, false, false, null),
        TRF__MVE_P(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Read, AuditMessages.EventOutcomeIndicator.Success,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, false, false, true, null),
        TRF__MVE_E(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Read, AuditMessages.EventOutcomeIndicator.MinorFailure,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, false, false, true, null),
        TRF__GET_P(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Read, AuditMessages.EventOutcomeIndicator.Success,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, false, true, false, null),
        TRF__GET_E(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Read, AuditMessages.EventOutcomeIndicator.MinorFailure,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, false, true, false, null),
        TRF__EXP_P(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Read, AuditMessages.EventOutcomeIndicator.Success,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, true, false, false, null),
        TRF__EXP_E(AuditMessages.EventID.DICOMInstancesTransferred, AuditMessages.EventActionCode.Read, AuditMessages.EventOutcomeIndicator.MinorFailure,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, true, false, false, null),

        DELETE_PAS(AuditMessages.EventID.DICOMInstancesAccessed, AuditMessages.EventActionCode.Delete, AuditMessages.EventOutcomeIndicator.Success,
                null, null, true, false, false, null),
        DELETE_ERR(AuditMessages.EventID.DICOMInstancesAccessed, AuditMessages.EventActionCode.Delete, AuditMessages.EventOutcomeIndicator.MinorFailure,
                null, null, true, false, false, null),

        PERM_DEL_E(AuditMessages.EventID.DICOMStudyDeleted, AuditMessages.EventActionCode.Delete, AuditMessages.EventOutcomeIndicator.MinorFailure,
                null, null, false, false, false, null),
        PERM_DEL_S(AuditMessages.EventID.DICOMStudyDeleted, AuditMessages.EventActionCode.Delete, AuditMessages.EventOutcomeIndicator.Success,
                null, null, false, false, false, null),

        APPLNSTART(AuditMessages.EventID.ApplicationActivity, AuditMessages.EventActionCode.Execute, AuditMessages.EventOutcomeIndicator.Success,
                null, null, false, false, false, AuditMessages.EventTypeCode.ApplicationStart),
        APPLN_STOP(AuditMessages.EventID.ApplicationActivity, AuditMessages.EventActionCode.Execute, AuditMessages.EventOutcomeIndicator.Success,
                null, null, false, false, false, AuditMessages.EventTypeCode.ApplicationStop),

        QUERY_QIDO(AuditMessages.EventID.Query, AuditMessages.EventActionCode.Execute, AuditMessages.EventOutcomeIndicator.Success,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, true, false, false, null),
        QUERY_FIND(AuditMessages.EventID.Query, AuditMessages.EventActionCode.Execute, AuditMessages.EventOutcomeIndicator.Success,
                AuditMessages.RoleIDCode.Source, AuditMessages.RoleIDCode.Destination, true, false, false, null),


        CONN__RJCT(AuditMessages.EventID.SecurityAlert, AuditMessages.EventActionCode.Execute,
                AuditMessages.EventOutcomeIndicator.MinorFailure, null, null, false, false, false, AuditMessages.EventTypeCode.NodeAuthentication);

        final AuditMessages.EventID eventID;
        final String eventActionCode;
        final String outcomeIndicator;
        final AuditMessages.RoleIDCode source;
        final AuditMessages.RoleIDCode destination;
        final boolean isSource;
        final boolean isDest;
        final boolean isOther;
        final EventTypeCode eventTypeCode;


        EventType(AuditMessages.EventID eventID, String eventActionCode, String outcome, AuditMessages.RoleIDCode source,
                  AuditMessages.RoleIDCode destination, boolean isSource, boolean isDest, boolean isOther, EventTypeCode etc) {
            this.eventID = eventID;
            this.eventActionCode = eventActionCode;
            this.outcomeIndicator = outcome;
            this.source = source;
            this.destination = destination;
            this.isSource = isSource;
            this.isDest = isDest;
            this.isOther = isOther;
            this.eventTypeCode = etc;
        }

        static EventType fromFile(Path file) {
            return valueOf(file.getFileName().toString().substring(0, 10));
        }

        static EventType forQuery(QueryContext ctx) {
            return (ctx.getHttpRequest() != null) ? QUERY_QIDO : QUERY_FIND;
        }

        static EventType forWADORetrieve(RetrieveContext ctx) {
            return ctx.getException() != null ? WADO_R_E__ : WADO_R_P__;
        }

        static EventType forInstanceStored(StoreContext ctx) {
            return ctx.getException() != null
                    ? ctx.getPreviousInstance() != null ? STORE_U_E_ : STORE_C_E_
                    : ctx.getLocation() != null
                    ? ctx.getPreviousInstance() != null ? STORE_U_P_ : STORE_C_P_
                    : null;
        }

        static EventType forBeginTransfer(RetrieveContext ctx) {
            EventType at = null;
            if (ctx.getException() != null) {
                if (ctx.isLocalRequestor())
                    at = BEGIN__E_E;
                if (!ctx.isDestinationRequestor() && !ctx.isLocalRequestor())
                    at = BEGIN__M_E;
                if (ctx.getRequestAssociation() != null && ctx.getStoreAssociation() != null && ctx.isDestinationRequestor())
                    at = BEGIN__G_E;
            } else {
                if (ctx.isLocalRequestor())
                    at = BEGIN__E_P;
                if (!ctx.isDestinationRequestor() && !ctx.isLocalRequestor())
                    at = BEGIN__M_P;
                if (ctx.getRequestAssociation() != null && ctx.getStoreAssociation() != null && ctx.isDestinationRequestor())
                    at = BEGIN__G_P;
            }
            return at;
        }

        static EventType forDicomInstTransferred(RetrieveContext ctx) {
            EventType at = null;
            if (ctx.getException() != null) {
                if (ctx.isLocalRequestor())
                    at = TRF__EXP_E;
                if (!ctx.isDestinationRequestor() && !ctx.isLocalRequestor())
                    at = TRF__MVE_E;
                if (ctx.getRequestAssociation() != null && ctx.getStoreAssociation() != null && ctx.isDestinationRequestor())
                    at = TRF__GET_E;
            } else {
                if (ctx.isLocalRequestor())
                    at = TRF__EXP_P;
                if (!ctx.isDestinationRequestor() && !ctx.isLocalRequestor())
                    at = TRF__MVE_P;
                if (ctx.getRequestAssociation() != null && ctx.getStoreAssociation() != null && ctx.isDestinationRequestor())
                    at = TRF__GET_P;
            }
            return at;
        }
    }

    public static void deleteFile(Path file) {
        try {
            Files.delete(file);
        } catch (IOException e) {
            LOG.warn("Failed to delete Audit Spool File - {}", file, e);
        }
    }

    public static class PatientStudyInfo {
        public static final int LOCAL_HOSTNAME = 0;
        public static final int REMOTE_HOSTNAME = 1;
        public static final int CALLING_AET = 2;
        public static final int CALLED_AET = 3;
        public static final int STUDY_UID = 4;
        public static final int ACCESSION_NO = 5;
        public static final int PATIENT_ID = 6;
        public static final int PATIENT_NAME = 7;
        public static final int OUTCOME = 8;

        private final String[] fields;

        public PatientStudyInfo(StoreContext ctx, Attributes attrs) {
            StoreSession session = ctx.getStoreSession();
            String outcome = (null != ctx.getException()) ? ctx.getException().getMessage(): null;
            fields = new String[] {
                    session.getArchiveAEExtension().getApplicationEntity().getDevice().getDeviceName(),
                    session.getRemoteHostName(),
                    session.getCallingAET(),
                    session.getCalledAET(),
                    ctx.getStudyInstanceUID(),
                    attrs.getString(Tag.AccessionNumber, ""),
                    attrs.getString(Tag.PatientID, ""),
                    attrs.getString(Tag.PatientName, ""),
                    outcome
            };
        }

        public PatientStudyInfo(RetrieveContext ctx, Attributes attrs) {
            String outcome = (null != ctx.getException()) ? ctx.getException().getMessage(): null;
            fields = new String[] {
                    ctx.getArchiveAEExtension().getApplicationEntity().getDevice().getDeviceName(),
                    ctx.getHttpRequest().getRemoteAddr(),
                    "",
                    ctx.getLocalAETitle(),
                    ctx.getStudyInstanceUIDs()[0],
                    attrs.getString(Tag.AccessionNumber, ""),
                    attrs.getString(Tag.PatientID, ""),
                    attrs.getString(Tag.PatientName, ""),
                    outcome
            };
        }

        public PatientStudyInfo(String s) {
            fields = StringUtils.split(s, '\\');
        }

        public String getField(int field) {
            return fields[field];
        }

        @Override
        public String toString() {
            return StringUtils.concat(fields, '\\');
        }
    }

    public static class InstanceInfo {
        public static final int CLASS_UID = 0;
        public static final int INSTANCE_UID = 1;
        public static final int MPPS_UID = 2;
        public static final int ACCESSION_NO = 3;

        private final String[] fields;

        public InstanceInfo(StoreContext ctx, Attributes attrs) {
            ArrayList<String> list = new ArrayList<>();
            list.add(ctx.getSopClassUID());
            list.add(ctx.getSopInstanceUID());
            list.add(StringUtils.maskNull(ctx.getMppsInstanceUID(), ""));
            Sequence reqAttrs = attrs.getSequence(Tag.RequestAttributesSequence);
            if (reqAttrs != null)
                for (Attributes reqAttr : reqAttrs) {
                    String accno = reqAttr.getString(Tag.AccessionNumber);
                    if (accno != null)
                        list.add(accno);
                }
            this.fields = list.toArray(new String[list.size()]);
        }

        public InstanceInfo(RetrieveContext ctx, Attributes attrs) {
            ArrayList<String> list = new ArrayList<>();
            list.add(attrs.getString(Tag.SOPClassUID));
            list.add(ctx.getSopInstanceUIDs()[0]);
            list.add("");
            Sequence reqAttrs = attrs.getSequence(Tag.RequestAttributesSequence);
            if (reqAttrs != null)
                for (Attributes reqAttr : reqAttrs) {
                    String accno = reqAttr.getString(Tag.AccessionNumber);
                    if (accno != null)
                        list.add(accno);
                }
            this.fields = list.toArray(new String[list.size()]);
        }

        public InstanceInfo(String s) {
            fields = StringUtils.split(s, '\\');
        }

        public String getField(int field) {
            return field < fields.length ? fields[field] : null;
        }

        @Override
        public String toString() {
            return StringUtils.concat(fields, '\\');
        }
    }

    public static class AccessionNumSopClassInfo {
        private final String accNum;
        private HashMap<String, HashSet<String>> sopClassMap = new HashMap<>();

        public AccessionNumSopClassInfo(String accNum) {
            this.accNum = accNum;
        }

        public String getAccNum() {
            return accNum;
        }
        public HashMap<String, HashSet<String>> getSopClassMap() {
            return sopClassMap;
        }
        public void addSOPInstance(RetrieveStudyInfo rInfo) {
            String cuid = rInfo.getField(RetrieveStudyInfo.SOPCLASSUID);
            HashSet<String> iuids = sopClassMap.get(cuid);
            if (iuids == null) {
                iuids = new HashSet<>();
                sopClassMap.put(cuid, iuids);
            }
            iuids.add(rInfo.getField(RetrieveStudyInfo.SOPINSTANCEUID));
        }
    }

    public static class RetrieveStudyInfo {
        public static final int STUDYUID = 0;
        public static final int ACCESSION = 1;
        public static final int SOPCLASSUID = 2;
        public static final int SOPINSTANCEUID = 3;
        public static final int PATIENTID = 4;
        public static final int PATIENTNAME = 5;

        private final String[] fields;
        public RetrieveStudyInfo(Attributes attrs) {
            fields = new String[] {
                    attrs.getString(Tag.StudyInstanceUID),
                    attrs.getString(Tag.AccessionNumber),
                    attrs.getString(Tag.SOPClassUID),
                    attrs.getString(Tag.SOPInstanceUID),
                    attrs.getString(Tag.PatientID, noValue),
                    attrs.getString(Tag.PatientName, noValue)
            };
        }
        public RetrieveStudyInfo(String s) {
            fields = StringUtils.split(s, '\\');
        }
        public String getField(int field) {
            return fields[field];
        }
        @Override
        public String toString() {
            return StringUtils.concat(fields, '\\');
        }
    }

    public static class RetrieveInfo {
        public static final int LOCALHOST = 0;
        public static final int LOCALAET = 1;
        public static final int DESTHOST = 2;
        public static final int DESTAET = 3;
        public static final int DESTNAPID = 4;
        public static final int DESTNAPCODE = 5;
        public static final int REQUESTORHOST = 6;
        public static final int MOVEAET = 7;
        public static final int OUTCOME = 8;

        private final String[] fields;

        public RetrieveInfo(RetrieveContext ctx) {
            String outcome = (null != ctx.getException()) ? ctx.getException().getMessage() : null;
            String destHost = (null != ctx.getDestinationHostName()) ? ctx.getDestinationHostName() : ctx.getDestinationAETitle();
            String destNapID = (null != ctx.getDestinationHostName()) ? ctx.getDestinationHostName() : null;
            String destNapCode = (null != ctx.getDestinationHostName()) ? AuditMessages.NetworkAccessPointTypeCode.IPAddress : null;
            fields = new String[] {
                    ctx.getLocalApplicationEntity().getDevice().getDeviceName(),
                    ctx.getLocalAETitle(),
                    destHost,
                    ctx.getDestinationAETitle(),
                    destNapID,
                    destNapCode,
                    ctx.getRequestorHostName(),
                    ctx.getMoveOriginatorAETitle(),
                    outcome
            };
        }

        public RetrieveInfo(String s) {
            fields = StringUtils.split(s, '\\');
        }

        public String getField(int field) {
            return fields[field];
        }

        @Override
        public String toString() {
            return StringUtils.concat(fields, '\\');
        }
    }

    public static class DeleteInfo {
        public static final int LOCALHOST = 0;
        public static final int LOCALAET = 1;
        public static final int REMOTEHOST = 2;
        public static final int REMOTEAET = 3;
        public static final int STUDYUID = 4;
        public static final int PATIENTID = 5;
        public static final int PATIENTNAME = 6;
        public static final int OUTCOME = 7;

        private final String[] fields;

        public DeleteInfo(StoreContext ctx) {
            String outcomeDesc = (ctx.getException() != null)
                    ? ctx.getRejectionNote().getRejectionNoteCode().getCodeMeaning() + " - " + ctx.getException().getMessage()
                    : ctx.getRejectionNote().getRejectionNoteCode().getCodeMeaning();
            fields = new String[]{
                    ctx.getStoreSession().getArchiveAEExtension().getApplicationEntity().getDevice().getDeviceName(),
                    ctx.getStoreSession().getCalledAET(),
                    ctx.getStoreSession().getRemoteHostName(),
                    ctx.getStoreSession().getCallingAET(),
                    ctx.getStudyInstanceUID(),
                    ctx.getAttributes().getString(Tag.PatientID, noValue),
                    ctx.getAttributes().getString(Tag.PatientID, noValue),
                    outcomeDesc
            };
        }

        public DeleteInfo(String s) {
            fields = StringUtils.split(s, '\\');
        }

        public String getField(int field) {
            return fields[field];
        }

        @Override
        public String toString() {
            return StringUtils.concat(fields, '\\');
        }
    }

    public static class DeleteStudyInfo {
        public static final int SOPCLASSUID = 0;
        public static final int NUMINSTANCES = 1;

        private final String[] fields;
        public DeleteStudyInfo(String cuid, String numInst) {
            ArrayList<String> list = new ArrayList<>();
            list.add(cuid);
            list.add(numInst);
            this.fields = list.toArray(new String[list.size()]);
        }
        public DeleteStudyInfo(String s) {
            fields = StringUtils.split(s, '\\');
        }
        public String getField(int field) {
            return field < fields.length ? fields[field] : null;
        }
        @Override
        public String toString() {
            return StringUtils.concat(fields, '\\');
        }
    }

    public static class PermanentDeletionInfo {
        public static final int STUDY_UID = 0;
        public static final int OUTCOME_DESC = 0;

        private final String[] fields;

        public PermanentDeletionInfo (StudyDeleteContext ctx) {
            String outcomeDesc = (ctx.getException() != null) ? ctx.getException().getMessage() : null;
            fields = new String[] {
                    ctx.getStudy().getStudyInstanceUID(),
                    outcomeDesc
            };
        }
        public PermanentDeletionInfo(String s) {
            fields = StringUtils.split(s, '\\');
        }

        public String getField(int field) {
            return fields[field];
        }

        @Override
        public String toString() {
            return StringUtils.concat(fields, '\\');
        }
    }


    public static class ConnectionRejectedInfo {
        public static final int REMOTE_ADDR = 0;
        public static final int LOCAL_ADDR = 1;
        public static final int PO_DESC = 2;
        public static final int OUTCOME_DESC = 3;
        private final String[] fields;

        public ConnectionRejectedInfo(Socket s, Throwable e) {
            fields = new String[] {
                    s.getRemoteSocketAddress().toString(),
                    s.getLocalSocketAddress().toString(),
                    e.getMessage(),
                    "MinorFailure"
            };
        }

        public ConnectionRejectedInfo(String s) {
            fields = StringUtils.split(s, '\\');
        }
        public String getField(int field) {
            return fields[field];
        }
        @Override
        public String toString() {
            return StringUtils.concat(fields, '\\');
        }
    }

    public static class QueryInfo {
        public static final int CALLING_AET = 0;
        public static final int REMOTE_HOST = 1;
        public static final int CALLED_AET = 2;
        public static final int LOCAL_HOST = 3;
        public static final int SOPCLASSUID = 4;
        public static final int PATIENT_ID = 5;
        public static final int QUERY_STRING = 6;

        private final String[] fields;

        public QueryInfo(QueryContext ctx) {
            String queryString = (ctx.getHttpRequest() != null)
                    ? ctx.getHttpRequest().getRequestURI() + ctx.getHttpRequest().getQueryString()
                    : null;
            String patientID = (ctx.getQueryKeys() != null && ctx.getQueryKeys().getString(Tag.PatientID) != null)
                    ? ctx.getQueryKeys().getString(Tag.PatientID) : noValue;
            fields = new String[] {
                    ctx.getCallingAET(),
                    ctx.getRemoteHostName(),
                    ctx.getCalledAET(),
                    ctx.getLocalApplicationEntity().getDevice().getDeviceName(),
                    ctx.getSOPClassUID(),
                    patientID,
                    queryString
            };
        }

        public QueryInfo(String s) {
            fields = StringUtils.split(s, '\\');
        }
        public String getField(int field) {
            return fields[field];
        }
        @Override
        public String toString() {
            return StringUtils.concat(fields, '\\');
        }
    }
}
