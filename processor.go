// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package taskqueue

// Processor works on a task. It must be registered in the Mangager.
type Processor func(args ...interface{}) error
