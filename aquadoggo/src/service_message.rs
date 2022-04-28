// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::service_manager::Sender;

pub type ServiceSender = Sender<ServiceMessage>;

#[derive(Clone)]
pub enum ServiceMessage {}
